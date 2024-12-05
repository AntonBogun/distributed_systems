#pragma once
#include "utils.h"
#include <mutex>
#include <atomic>
namespace distribsys{
    extern std::atomic<bool> _do_logging; //defined in main.cpp
    extern std::atomic<bool> _do_verbose; //defined in main.cpp
    extern std::mutex _mutex_logging;
    void set_logging(bool do_logging){
        std::lock_guard<std::mutex> lock(_mutex_logging);
        _do_logging = do_logging;
    }
    void log(const std::string &msg){
        std::lock_guard<std::mutex> lock(_mutex_logging);
        if(_do_logging){
            std::cout<<msg<<std::endl;
        }
    }
    void prefix_log(const std::string &prefix, const std::string &msg){
        log(prefix + msg);
    }
    void norm_log(const std::string &msg){
        prefix_log("==> HL: ", msg);
    }
    void verbose_log(const std::string &msg){
        prefix_log("==> HL: ", msg);
    }
    void debug_log(const std::string &msg){
        prefix_log("==> HL: ", msg);
    }
    void err_log(const std::string &msg){
        prefix_log("==> ERR: ", msg);
    }
    #define DEBUG_PRINTS 0
    #if DEBUG_PRINTS
    #define DEBUG_PRINT(x) debug_log(x)
    #else
    #define DEBUG_PRINT(x)
    #endif
    #define LINE_LOCATION " in " + __FILE__ + " at " + std::to_string(__LINE__)



}//namespace distribsys