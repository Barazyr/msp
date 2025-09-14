#ifndef SHARED_TIMER_MANAGER_HPP
#define SHARED_TIMER_MANAGER_HPP

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace msp {

struct ScheduledTask {
    std::function<void()> callback;
    std::chrono::steady_clock::time_point next_execution;
    std::chrono::microseconds period;
    bool active;
    
    ScheduledTask(std::function<void()> cb, std::chrono::microseconds p)
        : callback(cb), period(p), active(true) {
        next_execution = std::chrono::steady_clock::now() + period;
    }
};

/**
 * @brief Shared timer manager that batches multiple subscription requests
 * into a single timer thread to reduce resource contention and improve performance
 */
class SharedTimerManager {
public:
    static SharedTimerManager& getInstance();
    
    /**
     * @brief Schedule a callback to be executed periodically
     * @param callback Function to be called
     * @param period_seconds Period in seconds
     * @return Task ID for later removal
     */
    size_t scheduleTask(std::function<void()> callback, double period_seconds);
    
    /**
     * @brief Remove a scheduled task
     * @param task_id ID returned by scheduleTask
     * @return True if task was found and removed
     */
    bool removeTask(size_t task_id);
    
    /**
     * @brief Start the timer manager thread
     * @return True on success
     */
    bool start();
    
    /**
     * @brief Stop the timer manager thread
     * @return True on success
     */
    bool stop();
    
    /**
     * @brief Check if the timer manager is running
     * @return True if running
     */
    bool isRunning() const { return running_.load(); }

private:
    SharedTimerManager() = default;
    ~SharedTimerManager() { stop(); }
    
    // Prevent copying
    SharedTimerManager(const SharedTimerManager&) = delete;
    SharedTimerManager& operator=(const SharedTimerManager&) = delete;
    
    void timerLoop();
    void processPendingRequests();
    
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    std::shared_ptr<std::thread> timer_thread_;
    
    std::mutex tasks_mutex_;
    std::condition_variable cv_;
    std::map<size_t, std::shared_ptr<ScheduledTask>> tasks_;
    std::atomic<size_t> next_task_id_{1};
    
    // Batch processing for serial requests
    std::vector<std::function<void()>> pending_requests_;
    std::mutex requests_mutex_;
    std::chrono::steady_clock::time_point last_batch_time_;
    static constexpr std::chrono::microseconds BATCH_INTERVAL{1000}; // 1ms batching
};

}  // namespace msp

#endif // SHARED_TIMER_MANAGER_HPP