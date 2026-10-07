#include "data_tamer/data_sink.hpp"
#include "data_tamer/details/snapshot_pool.hpp"

#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <semaphore>
#include <stdexcept>
#include <thread>

namespace DataTamer
{
namespace
{
constexpr uint64_t kClosed = uint64_t{ 1 } << 63;
// Rounds over the attached queues in one delivery pass, before the worker looks
// again for attachments that came or went.
constexpr size_t kRoundsPerPass = 64;

// How long an idle worker keeps polling for a push before it marks itself
// asleep. While it polls, post() on the snapshot thread is an increment and a
// load; once it sleeps, the next post() releases the semaphore, a futex wake on
// the snapshot thread. 20 us covers the snapshots one control tick takes of
// several channels (each a few microseconds of serialization apart), so such a
// burst costs at most one wake, and it is about the window the replaced
// moodycamel semaphore spun (MAX_SEMA_SPINS = 10000 polls) before it blocked.
// The price is up to 20 us of one core per idle transition of the worker: 2 %
// of a core for a worker woken once per 1 kHz tick.
constexpr std::chrono::microseconds kSpinBeforeSleep{ 20 };

inline void cpuRelax()
{
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
  asm volatile("yield");
#endif
}

// Wakes the worker thread without a lock. post() costs one atomic increment and
// one load; only the post() that finds the worker asleep releases the semaphore
// (one futex wake). The worker sleeps on the semaphore's own counter. With
// libstdc++ 13 on Linux (_GLIBCXX_HAVE_PLATFORM_WAIT) that is a bare futex wait,
// outside the address-hashed waiter table of std::atomic::wait, so the
// notify_all() calls on the snapshot path (admission, channel epoch) never find
// a waiter and stay a load, and release() takes no lock. Other standard
// libraries, or targets without a platform wait (where libstdc++'s release()
// takes the waiter pool mutex), are correct but were not analysed for the
// real-time path.
class WorkerWake
{
public:
  void post()
  {
    // seq_cst on both sides: either sleep() sees this increment, or this load
    // sees `sleeping` set and the release below wakes it.
    sequence_.fetch_add(1, std::memory_order_seq_cst);
    if(sleeping_.load(std::memory_order_seq_cst) &&
       sleeping_.exchange(false, std::memory_order_seq_cst))
    {
      semaphore_.release();
    }
  }

  [[nodiscard]] uint32_t observe() const
  {
    return sequence_.load(std::memory_order_seq_cst);
  }

  // Worker only: polls for kSpinBeforeSleep. True if post() was called after
  // observe() returned `observed`.
  [[nodiscard]] bool spin(uint32_t observed) const
  {
    const auto deadline = std::chrono::steady_clock::now() + kSpinBeforeSleep;
    for(uint32_t polls = 1;; ++polls)
    {
      if(observe() != observed)
      {
        return true;
      }
      cpuRelax();
      // Reading the clock (vDSO, no syscall) every 64 polls keeps it cheap.
      if(polls % 64 == 0 && std::chrono::steady_clock::now() >= deadline)
      {
        return false;
      }
    }
  }

  // Blocks unless post() was called after observe() returned `observed`.
  void sleep(uint32_t observed)
  {
    sleeping_.store(true, std::memory_order_seq_cst);
    if(sequence_.load(std::memory_order_seq_cst) != observed &&
       sleeping_.exchange(false, std::memory_order_seq_cst))
    {
      return;  // withdrawn before any post() saw it
    }
    // Either nothing was posted since `observed`, or a post() took `sleeping`
    // and releases (or released) the semaphore: one release per sleep.
    semaphore_.acquire();
  }

private:
  std::atomic<uint32_t> sequence_{ 0 };
  std::atomic<bool> sleeping_{ false };
  std::binary_semaphore semaphore_{ 0 };
};
}  // namespace

//---------------- SnapshotRef ----------------

SnapshotRef::SnapshotRef(std::shared_ptr<SnapshotPool> pool, PoolSlot* slot)
  : pool_(std::move(pool)), slot_(slot)
{}

SnapshotRef::SnapshotRef(SnapshotRef&& other) noexcept
  : pool_(std::move(other.pool_)), slot_(other.slot_)
{
  other.slot_ = nullptr;
}

SnapshotRef& SnapshotRef::operator=(SnapshotRef&& other) noexcept
{
  if(this != &other)
  {
    reset();
    pool_ = std::move(other.pool_);
    slot_ = other.slot_;
    other.slot_ = nullptr;
  }
  return *this;
}

SnapshotRef::~SnapshotRef()
{
  reset();
}

SnapshotRef SnapshotRef::clone() const
{
  if(slot_)
  {
    SnapshotPool::addRef(slot_);
  }
  return SnapshotRef(pool_, slot_);
}

void SnapshotRef::reset()
{
  if(slot_)
  {
    SnapshotPool::release(slot_);
    slot_ = nullptr;
  }
  pool_.reset();
}

const Snapshot& SnapshotRef::operator*() const
{
  return slot_->snapshot;
}

const Snapshot* SnapshotRef::operator->() const
{
  return &slot_->snapshot;
}

//---------------- SinkWorker ----------------

/// Single-producer single-consumer ring of one channel on one worker. The
/// producer is the channel's snapshot thread (tryPush), the consumer whoever
/// holds store_mutex (the worker thread or drain()).
struct SinkWorker::Attachment
{
  // One entry more than the pool: an empty entry tells a full ring from an
  // empty one, without a division on the push path.
  explicit Attachment(size_t capacity)
    : size(capacity + 1), entries(std::make_unique<SnapshotRef[]>(capacity + 1))
  {}

  [[nodiscard]] size_t advance(size_t index) const
  {
    return index + 1 == size ? 0 : index + 1;
  }

  // Producer only. Never full in practice: every entry holds a distinct slot of
  // a pool of `size - 1` slots, and the snapshot being pushed holds one more.
  bool push(SnapshotRef&& snapshot)
  {
    const size_t at = tail.load(std::memory_order_relaxed);
    const size_t after = advance(at);
    if(after == cached_head)
    {
      cached_head = head.load(std::memory_order_acquire);
      if(after == cached_head)
      {
        return false;
      }
    }
    entries[at] = std::move(snapshot);  // the entry is empty: nothing is freed
    tail.store(after, std::memory_order_release);
    return true;
  }

  // Consumer only.
  bool pop(SnapshotRef& out)
  {
    const size_t at = head.load(std::memory_order_relaxed);
    if(at == cached_tail)
    {
      cached_tail = tail.load(std::memory_order_acquire);
      if(at == cached_tail)
      {
        return false;
      }
    }
    out = std::move(entries[at]);
    head.store(advance(at), std::memory_order_release);
    return true;
  }

  [[nodiscard]] bool empty() const
  {
    return head.load(std::memory_order_acquire) == tail.load(std::memory_order_acquire);
  }

  // Read-mostly line: written once, or only by control threads and the deliverer.
  const size_t size;
  std::unique_ptr<SnapshotRef[]> entries;
  // Set by detach(), after the channel's last push.
  std::atomic<bool> detached{ false };
  // Deliverer only: still served by the round robin of the current pass.
  bool serving = false;
  // Intrusive list of the worker's attachments, oldest first. attach() appends
  // under attachments_mutex; only a delivery pass unlinks. A pass walks it
  // unlocked up to the last node it saw under the lock, whose `next` is the
  // only link attach() writes meanwhile.
  std::unique_ptr<Attachment> next;
  // Consumer line: what the deliverer writes on every pop.
  alignas(64) std::atomic<size_t> head{ 0 };
  size_t cached_tail = 0;
  // Producer line, alone (the alignment pads the struct to whole lines).
  alignas(64) std::atomic<size_t> tail{ 0 };
  size_t cached_head = 0;
};

struct SinkWorker::Pimpl
{
  explicit Pimpl(std::unique_ptr<DataSink> owned) : sink(std::move(owned)) {}

  // First member, destroyed last: after the thread and every queued snapshot.
  std::unique_ptr<DataSink> sink;

  // Caller holds store_mutex.
  void deliver(DataSink& sink)
  {
    try
    {
      sink.onSnapshot(current_ref);
    }
    catch(const std::exception& e)
    {
      recordError(e.what());
    }
    catch(...)
    {
      recordError("unknown exception");
    }
    current_ref.reset();
  }

  void recordError(const char* what)
  {
    errors.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard lock(error_mutex);
    last_error = what;
  }

  // One delivery pass; caller holds store_mutex. Returns the number delivered.
  // Allocates nothing: it walks the intrusive list of attachments.
  size_t deliverPass(DataSink& sink)
  {
    Attachment* first = nullptr;
    Attachment* last = nullptr;
    {
      std::lock_guard lock(attachments_mutex);
      first = attachments.get();
      last = last_attachment;
    }
    const auto after = [last](const Attachment* attachment) {
      return attachment == last ? nullptr : attachment->next.get();
    };
    size_t delivered = 0;
    // Detached queues first, oldest first, each to the end (nothing is pushed
    // into them any more): their channel may already publish into a newer
    // queue on this worker, and per-channel order puts those snapshots after
    // these. removeDataSink() detaches under the channel's control mutex
    // before a later addDataSink() attaches, so a newer queue within
    // [first, last] implies its predecessors read as detached here; a queue
    // attached after this pass began lies beyond `last`.
    for(Attachment* attachment = first; attachment; attachment = after(attachment))
    {
      attachment->serving = !attachment->detached.load(std::memory_order_acquire);
      while(!attachment->serving && attachment->pop(current_ref))
      {
        deliver(sink);
        ++delivered;
      }
    }
    for(size_t round = 0; round < kRoundsPerPass; ++round)
    {
      size_t in_round = 0;
      for(Attachment* attachment = first; attachment; attachment = after(attachment))
      {
        if(attachment->serving && attachment->pop(current_ref))
        {
          deliver(sink);
          ++in_round;
        }
      }
      delivered += in_round;
      if(in_round == 0)
      {
        break;
      }
    }
    // detach() comes after the channel's last push, so detached and empty
    // means drained: free those queues.
    std::lock_guard lock(attachments_mutex);
    Attachment* kept = nullptr;
    for(auto* link = &attachments; *link;)
    {
      if((*link)->detached.load(std::memory_order_acquire) && (*link)->empty())
      {
        *link = std::move((*link)->next);
      }
      else
      {
        kept = link->get();
        link = &(*link)->next;
      }
    }
    last_attachment = kept;
    return delivered;
  }

  void wakeWorker() { wake.post(); }

  void startThread(DataSink& sink)
  {
    thread = std::jthread([this, &sink](std::stop_token stop) {
      while(true)
      {
        // Read before looking at the queues: a push after this read makes
        // sleep() below return instead of missing it.
        const uint32_t observed = wake.observe();
        if(stop.stop_requested())
        {
          break;
        }
        size_t delivered = 0;
        {
          std::unique_lock handoff(handoff_mutex);
          std::unique_lock lock(store_mutex);
          handoff.unlock();
          delivered = deliverPass(sink);
        }
        if(delivered == 0 && !wake.spin(observed))
        {
          wake.sleep(observed);
        }
      }
    });
  }

  void joinThread()
  {
    if(thread.joinable())
    {
      thread.request_stop();
      wakeWorker();
      thread.join();
    }
  }

  // A drainer claims handoff during the worker's callback, preventing the
  // worker from immediately barging back into store_mutex.
  std::mutex handoff_mutex;
  std::mutex store_mutex;
  SnapshotRef current_ref;  // store_mutex
  // The owned queues, attached and detached-but-not-yet-drained, oldest first.
  // Leaf lock: taken under store_mutex and under LogChannel's control mutex,
  // never the reverse.
  std::mutex attachments_mutex;
  std::unique_ptr<Attachment> attachments;
  Attachment* last_attachment = nullptr;  // attachments_mutex
  // Posted after every push, and by detach() and stop().
  WorkerWake wake;
  std::atomic<uint64_t> admission{ 0 };
  std::atomic<uint64_t> errors{ 0 };
  std::mutex error_mutex;
  std::string last_error;
  std::jthread thread;  // its stop token replaces a run flag
  Delivery delivery = Delivery::Threaded;
};

SinkWorker::SinkWorker(std::unique_ptr<DataSink> sink, Delivery delivery)
{
  if(!sink)
  {
    throw std::invalid_argument("SinkWorker: null sink");
  }
  _p = std::make_unique<Pimpl>(std::move(sink));
  _p->delivery = delivery;
  if(delivery == Delivery::Threaded)
  {
    _p->startThread(*_p->sink);
  }
}

SinkWorker::~SinkWorker()
{
  stop();
}

DataSink& SinkWorker::sink()
{
  return *_p->sink;
}

const DataSink& SinkWorker::sink() const
{
  return *_p->sink;
}

SinkWorker::Attachment* SinkWorker::attach(size_t capacity)
{
  auto attachment = std::make_unique<Attachment>(capacity);
  Attachment* appended = attachment.get();
  std::lock_guard lock(_p->attachments_mutex);
  (_p->last_attachment ? _p->last_attachment->next : _p->attachments) =
      std::move(attachment);
  _p->last_attachment = appended;
  return appended;
}

void SinkWorker::detach(Attachment* attachment) noexcept
{
  attachment->detached.store(true, std::memory_order_release);
  _p->wakeWorker();  // the worker delivers what is left and frees the queue
}

bool SinkWorker::tryPush(Attachment& attachment, SnapshotRef&& snapshot)
{
  // Announce first, check second: a transient increment on a closed sink is
  // withdrawn at once, and stop() waits for it like any other.
  struct AdmissionGuard
  {
    std::atomic<uint64_t>& admission;
    ~AdmissionGuard()
    {
      admission.fetch_sub(1, std::memory_order_release);
      admission.notify_all();  // cheap when nobody waits; wakes stop()
    }
  } guard{ _p->admission };
  if(_p->admission.fetch_add(1, std::memory_order_acq_rel) & kClosed)
    return false;
  // A failed push leaves snapshot intact; its owner releases it once.
  if(!attachment.push(std::move(snapshot)))
  {
    return false;
  }
  _p->wakeWorker();
  return true;
}

void SinkWorker::addSchema(const Schema& schema)
{
  std::lock_guard handoff(_p->handoff_mutex);
  std::lock_guard lock(_p->store_mutex);
  _p->sink->onSchema(schema);
}

void SinkWorker::stop()
{
  _p->admission.fetch_or(kClosed, std::memory_order_acq_rel);
  // Wait until every admitted push has finished: block on the counter instead
  // of spinning; each release notifies.
  for(auto state = _p->admission.load(std::memory_order_acquire); (state & ~kClosed) != 0;
      state = _p->admission.load(std::memory_order_acquire))
  {
    _p->admission.wait(state, std::memory_order_acquire);
  }
  _p->joinThread();
  drain();
}

void SinkWorker::start()
{
  if(_p->delivery == Delivery::Threaded && !_p->thread.joinable())
  {
    _p->startThread(*_p->sink);
  }
  _p->admission.fetch_and(~kClosed, std::memory_order_release);
}

void SinkWorker::drain()
{
  std::lock_guard handoff(_p->handoff_mutex);
  std::lock_guard lock(_p->store_mutex);
  while(_p->deliverPass(*_p->sink) != 0)
  {
  }
}

uint64_t SinkWorker::errors() const
{
  return _p->errors.load(std::memory_order_relaxed);
}

std::string SinkWorker::lastError() const
{
  std::lock_guard lock(_p->error_mutex);
  return _p->last_error;
}

}  // namespace DataTamer
