#include "SharedTimerManager.hpp"
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#endif

namespace msp {

SharedTimerManager& SharedTimerManager::getInstance() {
    static SharedTimerManager instance;
    return instance;
}

size_t SharedTimerManager::scheduleTask(std::function<void()> callback, double period_seconds) {
    if (period_seconds <= 0.0) return 0;
    
    auto period_us = std::chrono::microseconds(static_cast<size_t>(period_seconds * 1e6));
    auto task = std::make_shared<ScheduledTask>(callback, period_us);
    
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    size_t task_id = next_task_id_.fetch_add(1);
    tasks_[task_id] = task;
    
    // Notify timer thread of new task
    cv_.notify_one();
    
    return task_id;
}

bool SharedTimerManager::removeTask(size_t task_id) {
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    auto it = tasks_.find(task_id);
    if (it != tasks_.end()) {
        it->second->active = false;  // Mark for removal
        tasks_.erase(it);
        return true;
    }
    return false;
}

bool SharedTimerManager::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return false;  // Already running
    }
    
    stop_requested_.store(false);
    last_batch_time_ = std::chrono::steady_clock::now();
    
    timer_thread_ = std::make_shared<std::thread>([this] {
#ifdef __linux__
        // Set lower priority for timer thread
        struct sched_param param;
        param.sched_priority = 0;
        pthread_setschedparam(pthread_self(), SCHED_OTHER, &param);
        nice(3);  // Slightly lower priority
#endif
        timerLoop();
    });
    
    return true;
}

bool SharedTimerManager::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return false;  // Not running
    }
    
    stop_requested_.store(true);
    cv_.notify_all();
    
    if (timer_thread_ && timer_thread_->joinable()) {
        timer_thread_->join();
    }
    
    // Clear all tasks
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    tasks_.clear();
    
    return true;
}

void SharedTimerManager::timerLoop() {
    while (!stop_requested_.load()) {
        auto now = std::chrono::steady_clock::now();
        auto next_wake_time = now + std::chrono::milliseconds(10);  // Default 10ms
        
        // Process scheduled tasks
        {
            std::lock_guard<std::mutex> lock(tasks_mutex_);
            
            for (auto& [task_id, task] : tasks_) {
                if (!task->active || stop_requested_.load()) {
                    continue;
                }
                
                if (now >= task->next_execution) {
                    // Add callback to batch for serial processing
                    {
                        std::lock_guard<std::mutex> req_lock(requests_mutex_);
                        pending_requests_.push_back(task->callback);
                    }
                    
                    // Schedule next execution
                    task->next_execution = now + task->period;
                }
                
                // Update next wake time to earliest task
                if (task->next_execution < next_wake_time) {
                    next_wake_time = task->next_execution;
                }
            }
        }
        
        // Process batched requests periodically
        if (now - last_batch_time_ >= BATCH_INTERVAL) {
            processPendingRequests();
            last_batch_time_ = now;
        }
        
        // Wait until next task or stop signal
        std::unique_lock<std::mutex> lock(tasks_mutex_);
        cv_.wait_until(lock, next_wake_time, [this] { 
            return stop_requested_.load(); 
        });
    }
}

void SharedTimerManager::processPendingRequests() {
    std::vector<std::function<void()>> requests_to_process;
    
    // Move pending requests to local vector for processing
    {
        std::lock_guard<std::mutex> lock(requests_mutex_);
        if (!pending_requests_.empty()) {
            requests_to_process.swap(pending_requests_);
        }
    }
    
    // Process all batched requests sequentially to avoid serial port contention
    for (auto& request : requests_to_process) {
        if (stop_requested_.load()) break;
        
        try {
            request();
            // Small delay between serial requests to prevent port saturation
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        } catch (const std::exception&) {
            // Log error but continue processing other requests
            // Note: Exception details ignored to avoid unused variable warning
        }
    }
    
    // Yield CPU after batch processing
    std::this_thread::yield();
}

}  // namespace msp