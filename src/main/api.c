#include <stdio.h>
#include <signal.h>
#include <h2o.h>
#include <h2o/http1.h>
#include <h2o/http2.h>
#include <h2o/serverutil.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include "api.h"
#include "marlin.h"
#include "mlog.h"

#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wformat-truncation="

#define NUM_THREADS     24
#define NUM_LISTENERS   1
#define MAX_CONNECTIONS 8192

h2o_barrier_t startup_sync_barrier;
h2o_context_t *g_h2o_ctx = NULL;
static h2o_globalconf_t config;
static h2o_hostconf_t *hostconf;
static int _num_connections = 0;
static volatile sig_atomic_t shutdown_requested = 0;
static pthread_rwlock_t url_lock;
static pthread_rwlockattr_t rwlockattr;
static h2o_compress_args_t compress;

static int g_fd = 0;
static SSL_CTX *ssl_ctx = NULL;
static pthread_mutex_t *mutexes;

struct hthread {
    pthread_t tid;
    h2o_context_t ctx;
    h2o_multithread_receiver_t server_notifications;
};

struct listener_ctx_t {
    h2o_accept_ctx_t accept_ctx;
    h2o_socket_t *sock;
};

struct hthread threads[NUM_THREADS];


static void notify_all_threads(void) {
    unsigned i;
    for (i = 0; i < marlin->num_threads; i++) {
        h2o_multithread_message_t *message = h2o_mem_alloc(sizeof(*message));
        *message = (h2o_multithread_message_t){};
        h2o_multithread_send_message(&threads[i].server_notifications, message);
    }
}

static void on_sigterm(int signo) {
    shutdown_requested = 1;
    if (!h2o_barrier_done(&startup_sync_barrier)) {
        /* initialization hasn't completed yet, exit right away */
        exit(0);
    }
    M_ERR("sigterm handler, initiating shutdown");
    notify_all_threads();
}

static void setup_signal_handlers(void) {
    h2o_set_signal_handler(SIGTERM, on_sigterm);
    // TODO: Enable on release build.
    h2o_set_signal_handler(SIGINT, on_sigterm);
#ifdef VALGRIND_TEST
    h2o_set_signal_handler(SIGINT, on_sigterm);
#endif
    h2o_set_signal_handler(SIGPIPE, SIG_IGN);
}


static h2o_pathconf_t *register_handler(h2o_hostconf_t *hconf, const char *path, int (*on_req)(h2o_handler_t *, h2o_req_t *)) {
    h2o_pathconf_t *pathconf = h2o_config_register_path(hconf, path, 0);
    h2o_handler_t *handler = h2o_create_handler(pathconf, sizeof(*handler));
    handler->on_req = on_req;
    memset(&compress, 0, sizeof(compress));
    compress.gzip.quality = 6;
    h2o_compress_register(pathconf, &compress);
    return pathconf;
}

static int hello_world(h2o_handler_t *self, h2o_req_t *req) {
    static h2o_generator_t generator = {NULL, NULL};

    if (!h2o_memis(req->method.base, req->method.len, H2O_STRLIT("GET")))
        return -1;

    h2o_iovec_t body = h2o_strdup(&req->pool, "hello world\n", SIZE_MAX);
    req->res.status = 200;
    req->res.reason = "OK";
    h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL, H2O_STRLIT("text/plain"));
    h2o_start_response(req, &generator);
    h2o_send(req, &body, 1, 1);

    return 0;
}

static void on_server_notification(h2o_multithread_receiver_t *receiver, h2o_linklist_t *messages) {
    /* the notification is used only for exitting h2o_evloop_run; actual changes are done in the main loop of run_loop */
    //printf("on server notification\n");

    while (!h2o_linklist_is_empty(messages)) {
        h2o_multithread_message_t *message = H2O_STRUCT_FROM_MEMBER(h2o_multithread_message_t, link, messages->next);
        h2o_linklist_unlink(&message->link);
        free(message);
    }
}

static int num_connections(int delta) {
    return __sync_fetch_and_add(&_num_connections, delta);
}

static void on_socketclose(void *data) {
    int prev_num_connections = num_connections(-1);

    if (prev_num_connections == MAX_CONNECTIONS) {
        /* ready to accept new connections. wake up all the threads! */
        notify_all_threads();
    }
}

static void on_accept(h2o_socket_t *listener, const char *err) {
    struct listener_ctx_t *ctx = listener->data;
    size_t num_accepts = MAX_CONNECTIONS / 16 / marlin->num_threads;
    if (num_accepts < 8)
        num_accepts = 8;

    if (err != NULL) {
        return;
    }

    do {
        h2o_socket_t *sock;
        if (num_connections(0) >= MAX_CONNECTIONS) {
            /* The accepting socket is disactivated before entering the next in `run_loop`.
             * Note: it is possible that the server would accept at most `max_connections + num_threads` connections, since the
             * server does not check if the number of connections has exceeded _after_ epoll notifies of a new connection _but_
             * _before_ calling `accept`.  In other words t/40max-connections.t may fail.
             */
            break;
        }
        if ((sock = h2o_evloop_socket_accept(listener)) == NULL) {
            break;
        }
        num_connections(1);

        sock->on_close.cb = on_socketclose;
        sock->on_close.data = ctx->accept_ctx.ctx;

        h2o_accept(&ctx->accept_ctx, sock);

    } while (--num_accepts != 0);
}


static int create_listener(void) {

    struct sockaddr_in addr;
    int reuseaddr_flag = 1;
    // h2o_socket_t *sock;


    { /* raise RLIMIT_NOFILE */
        struct rlimit limit;
        if (getrlimit(RLIMIT_NOFILE, &limit) == 0) {
            limit.rlim_cur = limit.rlim_max;
            if (setrlimit(RLIMIT_NOFILE, &limit) == 0
#ifdef __APPLE__
                || (limit.rlim_cur = OPEN_MAX, setrlimit(RLIMIT_NOFILE, &limit)) == 0
#endif
                ) {
                M_DBG("Raised RLIMIT_NOFILE to %d", (int)limit.rlim_cur);
            }
        }
    }


    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(marlin->port);

    if ((g_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1 ||
            setsockopt(g_fd, SOL_SOCKET, SO_REUSEADDR, &reuseaddr_flag, sizeof(reuseaddr_flag)) != 0 ||
            bind(g_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(g_fd, SOMAXCONN) != 0) {
        return -1;
    }

    return 0;
}

static void set_cloexec(int fd) {
    if (fcntl(fd, F_SETFD, FD_CLOEXEC) == -1) {
        perror("failed to set FD_CLOEXEC");
        abort();
    }
}

static void update_listener_state(struct listener_ctx_t *listeners) {
    size_t i;

    if (num_connections(0) < MAX_CONNECTIONS) {
        for (i = 0; i != NUM_LISTENERS; ++i) {
            if (!h2o_socket_is_reading(listeners[i].sock))
                h2o_socket_read_start(listeners[i].sock, on_accept);
        }
    } else {
        for (i = 0; i != NUM_LISTENERS; ++i) {
            if (h2o_socket_is_reading(listeners[i].sock))
                h2o_socket_read_stop(listeners[i].sock);
        }
    }
}

void *run_loop(void *_thread_index) {
    size_t thread_index = (size_t)_thread_index;
    struct listener_ctx_t *listeners = alloca(sizeof(*listeners) * NUM_LISTENERS);
    size_t i;
    M_DBG("RUN LOOP for %u", thread_index);
    
#ifdef __linux__
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET(thread_index, &cpus);
    sched_setaffinity(pthread_self(), sizeof(cpu_set_t), &cpus);
#endif

    // should never happen, but gcc on Ubuntu 14.04-LTS warns about this when run with -Ofast, so this makes it happy (& on the off chance it finds a way to execute this, we'll rather abort as things are bonkers)
    if (thread_index > marlin->num_threads)
        abort();

    h2o_context_init(&threads[thread_index].ctx, h2o_evloop_create(), &config);
    h2o_multithread_register_receiver(threads[thread_index].ctx.queue, 
            &threads[thread_index].server_notifications, on_server_notification);

    threads[thread_index].tid = pthread_self();
    // First thread is the global context
    if (thread_index == 1) {
        g_h2o_ctx = &threads[thread_index].ctx;
    }

    /* setup listeners */
    for (i = 0; i < NUM_LISTENERS; ++i) {
        int fd;
        /* dup the listener fd for other threads than the main thread */
        if (thread_index == 0) {
            fd = g_fd;
        } else {
            if ((fd = dup(g_fd)) == -1) {
                M_ERR("failed to dup listening socket");
                abort();
            }
            set_cloexec(fd);
        }
        memset(listeners + i, 0, sizeof(listeners[i]));
        listeners[i].accept_ctx.ctx = &threads[thread_index].ctx;
        listeners[i].accept_ctx.hosts = config.hosts;
        if (ssl_ctx) {
            M_DBG("Setting up ssl context for listener");
            listeners[i].accept_ctx.ssl_ctx = ssl_ctx;
        }
        listeners[i].sock = h2o_evloop_socket_create(threads[thread_index].ctx.loop, fd, H2O_SOCKET_FLAG_DONT_READ);
        listeners[i].sock->data = listeners + i;
    }
    /* and start listening */
    update_listener_state(listeners);
    // Kickstart all threads
    if (thread_index == 0)
        notify_all_threads();

    /* make sure all threads are initialized before starting to serve requests */
    h2o_barrier_wait(&startup_sync_barrier);

    /* the main loop */
    while (1) {
        // Shutdown
        if (shutdown_requested) break;
        update_listener_state(listeners);
        /* run the loop once */
        h2o_evloop_run(threads[thread_index].ctx.loop, INT32_MAX);
    }

    //printf("Shutdown thread %u\n", thread_index);

    if (thread_index == 0)
        M_ERR("received SIGTERM, gracefully shutting down\n");

    /* shutdown requested, unregister, close the listeners and notify the protocol handlers */
    for (i = 0; i != NUM_LISTENERS; ++i)
        h2o_socket_read_stop(listeners[i].sock);
    h2o_evloop_run(threads[thread_index].ctx.loop, 0);
    for (i = 0; i != NUM_LISTENERS; ++i) {
        h2o_socket_close(listeners[i].sock);
        listeners[i].sock = NULL;
    }
    h2o_context_request_shutdown(&threads[thread_index].ctx);

    /* wait until all the connection gets closed */
    while (num_connections(0) != 0)
        h2o_evloop_run(threads[thread_index].ctx.loop, INT32_MAX);

    if (thread_index == 0) {
        sleep(1);
        M_INFO("Shutting down h2o request thread");
    } else {
        pthread_exit(0);
    }
    return NULL;
}

inline static void lock_callback(int mode, int n, const char *file, int line) {
    if ((mode & CRYPTO_LOCK) != 0) {
        pthread_mutex_lock(mutexes + n);
    } else if ((mode & CRYPTO_UNLOCK) != 0) {
        pthread_mutex_unlock(mutexes + n);
    } else {
        assert(!"unexpected mode");
    }
}

inline static unsigned long thread_id_callback(void) {
    return (unsigned long)pthread_self();
}

void init_openssl(void) {
    int nlocks = CRYPTO_num_locks(), i;
    mutexes = h2o_mem_alloc(sizeof(*mutexes) * nlocks);
    for (i = 0; i != nlocks; ++i)
        pthread_mutex_init(mutexes + i, NULL);
    CRYPTO_set_locking_callback(lock_callback);
    CRYPTO_set_id_callback(thread_id_callback);
    /* TODO [OpenSSL] set dynlock callbacks for better performance */
    SSL_load_error_strings();
    SSL_library_init();
    OpenSSL_add_all_algorithms();
}

//static void setup_ssl(void) __attribute__ ((unused));
static void setup_ssl(void) {
    init_openssl();
    ssl_ctx = SSL_CTX_new(SSLv23_server_method());
    SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2);
    //setup_ecc_key(ssl_ctx);
    if (SSL_CTX_use_certificate_chain_file(ssl_ctx, marlin->ssl_cert) != 1) {
        M_ERR("Failed to load certificate chain");
    }
    if (SSL_CTX_use_PrivateKey_file(ssl_ctx, marlin->ssl_key, SSL_FILETYPE_PEM) != 1) {
        M_ERR("Failed to load private key");
    }
}

void init_api(void) {
      setup_signal_handlers();
      if (marlin->https) {
        setup_ssl();
      }

      h2o_config_init(&config);

      hostconf = h2o_config_register_host(&config, h2o_iovec_init(H2O_STRLIT("default")), 65535);

      pthread_rwlockattr_init(&rwlockattr);
#ifdef __linux__
      pthread_rwlockattr_setkind_np(&rwlockattr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif
      pthread_rwlock_init(&url_lock, &rwlockattr);

      register_handler(hostconf, "/hello-world", hello_world);

      if (create_listener() != 0) {
          M_ERR("failed to listen to 127.0.0.1:9002: %s", strerror(errno));
          goto Error;
      }

      h2o_barrier_init(&startup_sync_barrier, marlin->num_threads);
      for (int i = 1; i < marlin->num_threads; i++) {
          pthread_t tid;
          h2o_multithread_create_thread(&tid, NULL, run_loop, (void *)i);
      }
Error:
      return;
}
