#include <iostream>
#include "mtcp.hpp"

int main(){

    mtcp::proactor_engine engine;
    engine.add_server("", "0.0.0.0:9000", nullptr);
    engine.run();
    return 0;
}