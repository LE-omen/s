// V12 question: can independent SQL sessions progress on one bounded pipe pair?
// Included by the composition unit. One reader dispatches; executors never read
// a process pipe. A request has one credited frame plus a reserved terminal slot.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include "lib/thread/thread_pool.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/profile/ob_trace_id.h"
namespace oceanbase { namespace observer { namespace namespace_worker_prototype {
// Protocol ceiling only; vectors still grow on demand and no per-client slots
// are preallocated. Keep one admission slot for control traffic.
constexpr size_t MAX_REQUESTS = 256;
struct PendingRequest;
struct DirectInsertOwner;
thread_local std::function<void(PendingRequest &, bool, bool)> worker_wait;
thread_local const common::ObCurTraceId::TraceId *worker_call_trace = nullptr;
struct PendingRequest {
  RequestTag tag;
  common::ObCurTraceId::TraceId call_trace;
  sql::ObSQLSessionInfo *sql_session = nullptr; // Worker-local owner; never sent over IPC.
  // A PX route attaches once to its SQC's active storage session. This weak
  // reference cannot prolong storage lifetime after all execution owners leave.
  std::weak_ptr<DirectInsertOwner> direct_insert;
  std::mutex mutex;
  std::condition_variable changed;
  std::optional<Frame> incoming, terminal;
  bool credit = true, reported_wait = false;
  int64_t deadline = INT64_MAX; // absolute query deadline, shared by both processes
  bool cancellable = false;
  std::atomic<int> cancelled{common::OB_SUCCESS};
  int error = common::OB_SUCCESS;
  // Installed before sending the request. Storage frames are scheduled on the
  // shared runtime, independently of the thread consuming SQL results.
  std::function<int(Frame)> dispatch_storage;
  explicit PendingRequest(RequestTag t) : tag(t) {}
  int status() const {
    const int ret = cancelled.load();
    return ret ? ret : common::ObTimeUtility::current_time() >= deadline ? common::OB_TIMEOUT : common::OB_SUCCESS;
  }
  void cancel(int ret) {
    std::lock_guard<std::mutex> guard(mutex);
    int expected = common::OB_SUCCESS; cancelled.compare_exchange_strong(expected, ret);
    changed.notify_all();
  }
  void fail(int ret) {
    std::lock_guard<std::mutex> guard(mutex);
    if (!error) { error = ret; }
    int expected = common::OB_SUCCESS; cancelled.compare_exchange_strong(expected, ret);
    changed.notify_all();
  }
  int post(Frame frame) {
    std::lock_guard<std::mutex> guard(mutex);
    if (error) { return common::OB_SUCCESS; }
    if (frame.type() == 'K') {
      if (credit || !frame.consumed()) { return common::OB_INVALID_ARGUMENT; }
      credit = true;
    } else {
      auto &slot = frame.type() == 'D' ? terminal : incoming;
      if (slot) { return common::OB_SIZE_OVERFLOW; }
      slot = std::move(frame);
    }
    changed.notify_all(); return common::OB_SUCCESS;
  }
  int take(Frame &frame, bool draining = false) {
    if (worker_wait) { worker_wait(*this, false, draining); }
    std::unique_lock<std::mutex> guard(mutex);
    auto ready = [&] { return error || incoming || terminal || (!draining && cancelled.load()); };
    if (draining || deadline == INT64_MAX) {
      changed.wait(guard, ready);
    } else if (!changed.wait_until(guard,
        std::chrono::system_clock::time_point(std::chrono::microseconds(deadline)), ready)) {
      return common::OB_TIMEOUT;
    }
    if (error) { return error; }
    const int state = draining ? common::OB_SUCCESS : status();
    if (state) { return state; }
    auto &slot = incoming ? incoming : terminal;
    frame = std::move(*slot); slot.reset(); return common::OB_SUCCESS;
  }
  int take_credit(bool draining = false) {
    if (worker_wait) { worker_wait(*this, true, draining); }
    std::unique_lock<std::mutex> guard(mutex);
    if (!credit && !error && !reported_wait) {
      reported_wait = true;
      fprintf(stderr, "PROTOTYPE_V12_CREDIT_WAIT request=%llu generation=%llu\n",
          (unsigned long long)tag.slot, (unsigned long long)tag.generation);
    }
    auto ready = [&] { return error || credit || (!draining && cancelled.load()); };
    if (draining || deadline == INT64_MAX) {
      changed.wait(guard, ready);
    } else if (!changed.wait_until(guard,
        std::chrono::system_clock::time_point(std::chrono::microseconds(deadline)), ready)) {
      return common::OB_TIMEOUT;
    }
    if (error) { return error; }
    const int state = draining ? common::OB_SUCCESS : status();
    if (state) { return state; }
    credit = false; return common::OB_SUCCESS;
  }
};
struct RequestRoutes {
  struct Slot { uint64_t generation = 0; std::shared_ptr<PendingRequest> request; };
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<Slot> slots;
  std::vector<uint64_t> free;
  size_t active = 0;
  bool closed = false;
  const uint64_t origin;
  explicit RequestRoutes(uint64_t request_origin = 0) : origin(request_origin) {}
  std::shared_ptr<PendingRequest> allocate(bool control) {
    std::unique_lock<std::mutex> guard(mutex);
    auto available = [&] {
      return closed || ((!free.empty() || slots.size() < MAX_REQUESTS)
          && (control || active < MAX_REQUESTS - 1));
    };
    // Leave an admission slot for close/ping even when SQL admission is full.
    // Only the calling login/disconnect task waits; the receiver never does.
    if (control) { changed.wait_for(guard, std::chrono::seconds(30), available); }
    if (closed || !available()) { return {}; }
    uint64_t index;
    if (free.empty()) { index = slots.size(); slots.emplace_back(); }
    else { index = free.back(); free.pop_back(); }
    auto &slot = slots[index];
    slot.request = std::make_shared<PendingRequest>(RequestTag{origin | index, ++slot.generation});
    ++active;
    return slot.request;
  }
  std::shared_ptr<PendingRequest> accept(RequestTag tag) {
    std::lock_guard<std::mutex> guard(mutex);
    const uint64_t index = tag.slot & ~WORKER_REQUEST;
    if (closed || (tag.slot & WORKER_REQUEST) != origin || index >= MAX_REQUESTS || !tag.generation) { return {}; }
    if (slots.size() <= index) { slots.resize(index + 1); }
    auto &slot = slots[index];
    if (slot.request || tag.generation <= slot.generation) { return {}; }
    slot.generation = tag.generation;
    ++active;
    slot.request = std::make_shared<PendingRequest>(tag); return slot.request;
  }
  std::shared_ptr<PendingRequest> find(RequestTag tag) {
    std::lock_guard<std::mutex> guard(mutex);
    const uint64_t index = tag.slot & ~WORKER_REQUEST;
    return (tag.slot & WORKER_REQUEST) == origin && index < slots.size() && slots[index].generation == tag.generation
        ? slots[index].request : nullptr;
  }
  void release(RequestTag tag, bool reuse = false) {
    std::lock_guard<std::mutex> guard(mutex);
    const uint64_t index = tag.slot & ~WORKER_REQUEST;
    if ((tag.slot & WORKER_REQUEST) == origin && index < slots.size()
        && slots[index].generation == tag.generation && slots[index].request) {
      slots[index].request.reset();
      --active;
      if (reuse && tag.generation != UINT64_MAX) { free.push_back(index); }
      changed.notify_one();
    }
  }
  void fail() {
    std::lock_guard<std::mutex> guard(mutex); closed = true;
    for (auto &slot : slots) { if (slot.request) { slot.request->fail(common::OB_CONNECT_ERROR); } }
    changed.notify_all();
  }
};
// Reuse native thread creation/context setup. No thread is created per session.
class PrototypeThreads : public lib::ThreadPool {
public:
  std::function<void()> body;
  PrototypeThreads(int64_t count, std::function<void()> fn) : lib::ThreadPool(count), body(std::move(fn)) {}
  void run1() override { body(); }
};
thread_local PendingRequest *worker_request = nullptr;
RequestRoutes worker_storage_routes{WORKER_REQUEST};
} } }
