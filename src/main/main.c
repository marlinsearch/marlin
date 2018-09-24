#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "marlin.h"
#include "api.h"

int main(int argc, char **argv) {
    M_INFO("Initializing ...");

    // Load settings file
    char settings[PATH_MAX];
    snprintf(settings, sizeof(settings), "%s", SETTINGS_PATH);

    int opt = 0;
    // Note : Keep options to minimum, probably just a version, help
    // and settings path
    while((opt = getopt(argc, argv, "c:")) != -1) {
        switch(opt) {
            case 'c':
                snprintf(settings, sizeof(settings), "%s", optarg);
                break;
            default:
                exit(EXIT_FAILURE);
        }
    }

    // Load the configuration before doing anything else
    load_settings(settings);

    // This should start the api threads
    init_api();

    sleep(1);

    init_marlin();

    run_loop((void *)0);

    shutdown_marlin();
}
