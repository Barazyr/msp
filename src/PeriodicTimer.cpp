#include "PeriodicTimer.hpp"

namespace msp {

PeriodicTimer::PeriodicTimer(std::function<void()> funct,
                             const double period_seconds) :
    funct(funct) {
    period_us =
        std::chrono::duration<size_t, std::micro>(size_t(period_seconds * 1e6));
}

bool PeriodicTimer::start() {
    // only start thread if period is above 0
    if(!(period_us.count() > 0)) return false;
    
    // only start once
    bool expected = false;
    if(!running_.compare_exchange_strong(expected, true)) {
        return false; // already running
    }
    
    stop_requested_.store(false);
    
    // start the thread
    thread_ptr = std::shared_ptr<std::thread>(new std::thread([this] {
        auto next_execution = std::chrono::steady_clock::now();
        
        while(!stop_requested_.load()) {
            // call function
            funct();
            
            // calculate next execution time
            next_execution += period_us;
            
            // wait until next execution time or stop is requested
            std::unique_lock<std::mutex> lock(mutex_);
            if(cv_.wait_until(lock, next_execution, [this] { return stop_requested_.load(); })) {
                // stop was requested
                break;
            }
            // if we wake up due to timeout (normal case), continue the loop
        }
    }));
    return true;
}

bool PeriodicTimer::stop() {
    bool expected = true;
    if(!running_.compare_exchange_strong(expected, false)) {
        return false; // wasn't running
    }
    
    // signal the thread to stop
    stop_requested_.store(true);
    cv_.notify_all();
    
    // wait for thread to finish
    if(thread_ptr != nullptr && thread_ptr->joinable()) {
        thread_ptr->join();
    }
    
    return true;
}

void PeriodicTimer::setPeriod(const double& period_seconds) {
    bool was_running = running_.load();
    
    if(was_running) {
        stop();
    }
    
    period_us =
        std::chrono::duration<size_t, std::micro>(size_t(period_seconds * 1e6));
    
    if(was_running) {
        start();
    }
}

}  // namespace msp