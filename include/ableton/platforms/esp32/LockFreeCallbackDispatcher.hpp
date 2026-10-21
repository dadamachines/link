/* Copyright 2020, Ableton AG, Berlin. All rights reserved.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  If you would like to incorporate Link into a proprietary software application,
 *  please contact <link-devs@ableton.com>.
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

namespace ableton
{
namespace platforms
{
namespace esp32
{

// Utility to signal invocation of a callback on another thread in a lock free manner.
// The callback is evoked on a thread owned by the instance of this class.
//
// invoke() is a FreeRTOS task notification: it never blocks and never takes a lock
// the dispatching task can hold, so a real-time caller cannot wait on it. The task
// also runs the callback after fallbackPeriod without a notification, which gives
// the guaranteed minimum signaling rate.

template <typename Callback, typename Duration>
class LockFreeCallbackDispatcher
{
public:
  LockFreeCallbackDispatcher(Callback callback, Duration fallbackPeriod)
    : mCallback(std::move(callback))
    , mFallbackTicks(std::max<TickType_t>(
        1,
        pdMS_TO_TICKS(
          std::chrono::duration_cast<std::chrono::milliseconds>(fallbackPeriod).count())))
    , mRunning(true)
  {
    xTaskCreatePinnedToCore(run, "link", 4096, this, tskIDLE_PRIORITY, &mTaskHandle, LINK_ESP_TASK_CORE_ID);
  }

  ~LockFreeCallbackDispatcher()
  {
    mRunning = false;
    invoke();
    vTaskDelete(mTaskHandle);
  }

  void invoke()
  {
    if (mTaskHandle != nullptr)
    {
      xTaskNotifyGive(mTaskHandle);
    }
  }

private:
  static void run(void* userData)
  {
    auto dispatcher = static_cast<LockFreeCallbackDispatcher*>(userData);
    while (dispatcher->mRunning.load())
    {
      ulTaskNotifyTake(pdTRUE, dispatcher->mFallbackTicks);
      dispatcher->mCallback();
      vTaskDelay(1);
    }
  }

  Callback mCallback;
  TickType_t mFallbackTicks;
  std::atomic<bool> mRunning;
  TaskHandle_t mTaskHandle = nullptr;
};

} // namespace esp32
} // namespace platforms
} // namespace ableton
