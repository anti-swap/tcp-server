// Compile-only translation unit: instantiates the header with the Linux-only
// platform shim so g++ can type-check it on a non-Linux host.
// Build: see verify/build.sh
#define __linux__ 1
#include "../tcp-server-mt.h"

int main() {
    industrial::MultiReactorServer::Config cfg;
    cfg.port = 0;
    cfg.numReactors = 2;
    industrial::MultiReactorServer server(cfg);
    return server.start() ? 0 : 1;
}
