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
        prefix_log("DEBUG: ", msg);
    }
    void err_log(const std::string &msg){
        prefix_log("==> ERR: ", msg);
    }
    #define DEBUG_PRINTS 1
    #if DEBUG_PRINTS
    #define DEBUG_PRINT(x) debug_log(x)
    #else
    #define DEBUG_PRINT(x)
    #endif
    #define LINE_LOCATION " in " + __FILE__ + " at " + std::to_string(__LINE__)

    #define throw_if(condition, message)                                          \
    {                                                                             \
        bool TMP_RET;                                                             \
        try {                                                                     \
            TMP_RET = condition;                                                  \
        } catch (std::exception &e) {                                             \
            throw std::runtime_error(std::string(e.what()) + " >>> " +            \
                                std::string(message) + "; in file " + __FILE__ +  \
                                " at line " + std::to_string(__LINE__));          \
        }                                                                         \
        if (TMP_RET) {                                                            \
            throw std::runtime_error(std::string(message) +                       \
                                        "; in file " + __FILE__ +                \
                                        " at line " + std::to_string(__LINE__)); \
        }                                                                         \
    }
    constexpr bool THROW_ON_RECOVERABLE = false;
    //> This macro is used to handle recoverable errors in a consistent manner.
    //> If THROW_ON_RECOVERABLE is defined, it will throw an exception on error.
    //> Otherwise, it will log the error and execute a default action.
    //> It also catches exceptions and rethrows them with additional context.

    #define THROW_IF_RECOVERABLE(x, s, default_action)                        \
    {                                                                         \
        bool TMP_RET;                                                         \
        try {                                                                 \
            TMP_RET = x;                                                      \
        } catch (std::exception &e) {                                         \
            throw std::runtime_error(std::string(e.what()) + " >>> " +        \
                                    std::string(s) +"; in file " + __FILE__ + \
                                    " at line " + std::to_string(__LINE__));  \
        }                                                                     \
        if (TMP_RET) {                                                        \
            if constexpr (THROW_ON_RECOVERABLE) {                             \
                throw std::runtime_error(std::string(s) +                     \
                                        "; in file " + __FILE__ +            \
                                        " at line " + std::to_string(__LINE__)); \
            } else {                                                          \
                err_log(std::string(s) +                                      \
                        "; in file " + __FILE__ +                             \
                        " at line " + std::to_string(__LINE__));              \
                default_action                                                \
            }                                                                 \
        }                                                                     \
    }

}//namespace distribsys