#include <stdlib.h>
#include "app.h"

/* Creates a new app or loads an existing app */
struct app *app_new(const char *name, const char *appid, const char *apikey) {
    struct app *a = malloc(sizeof(struct app));
    snprintf(a->name, sizeof(a->name), "%s", name);
    snprintf(a->appid, sizeof(a->appid), "%s", appid);
    snprintf(a->apikey, sizeof(a->apikey), "%s", apikey);

    return a;
}

void app_free(struct app *a) {
    free(a);
}
