#include "utils.h"
#include <mutex>
#include <atomic>
namespace distribsys{
    extern std::atomic<bool> _do_logging; //defined in main.cpp
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



}//namespace distribsys