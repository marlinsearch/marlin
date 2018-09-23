default: lmdb utf8proc jansson jemalloc h2o

# Prerequisites target
.make-prerequisites:
	@touch $@

jemalloc: .make-prerequisites
	cd jemalloc && ./configure --with-version=5.1.0-0-g0 --with-lg-quantum=3 --with-jemalloc-prefix=je_ --enable-cc-silence CFLAGS="$(JEMALLOC_CFLAGS)" LDFLAGS="$(JEMALLOC_LDFLAGS)"
	cd jemalloc && $(MAKE) CFLAGS="$(JEMALLOC_CFLAGS)" LDFLAGS="$(JEMALLOC_LDFLAGS)" lib/libjemalloc.a

.PHONY: jemalloc

lmdb: .make-prerequisites
	cd lmdb && make

.PHONY: lmdb

utf8proc: .make-prerequisites
	cd utf8proc && make

.PHONY: utf8proc

jansson: .make-prerequisites
	cd jansson && autoreconf -fi && ./configure && make

.PHONY: jansson

h2o: .make-prerequisites
	cd h2o && cmake . && make

.PHONY: h2o
