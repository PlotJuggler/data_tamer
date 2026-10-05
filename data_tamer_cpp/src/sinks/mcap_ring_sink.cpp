#include "data_tamer/sinks/mcap_ring_sink.hpp"

// The MCAP implementation is compiled in mcap_sink.cpp.
#include "data_tamer/sinks/mcap_encoding.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace DataTamer
{
namespace
{
using std::chrono::nanoseconds;

// requestDump() word: the top bit means "pending", the other bits hold the
// post-trigger delay in nanoseconds. Zero means no active request.
constexpr uint64_t kPending = uint64_t{ 1 } << 63;
constexpr uint64_t kMaxDelay = kPending - 1;

nanoseconds saturatingAdd(nanoseconds a, nanoseconds b)
{
  const auto max = std::numeric_limits<nanoseconds::rep>::max();
  return (b.count() > 0 && a.count() > max - b.count()) ? nanoseconds(max) : a + b;
}

nanoseconds saturatingSub(nanoseconds a, nanoseconds b)
{
  const auto min = std::numeric_limits<nanoseconds::rep>::min();
  return (b.count() > 0 && a.count() < min + b.count()) ? nanoseconds(min) : a - b;
}

// Each stored snapshot is this header followed by mask and payload bytes.
// Records are packed back to back and copied with memcpy, so no alignment.
struct RecordHeader
{
  uint64_t schema_hash;
  int64_t timestamp;
  uint32_t mask_size;
  uint32_t payload_size;

  size_t recordSize() const { return sizeof(RecordHeader) + mask_size + payload_size; }
};

// Byte ring of records. A record may wrap around the end of the buffer.
class ByteRing
{
public:
  explicit ByteRing(size_t capacity) : buffer_(capacity) {}

  size_t capacity() const { return buffer_.size(); }
  size_t used() const { return used_; }
  size_t count() const { return count_; }

  RecordHeader front() const
  {
    RecordHeader header;
    read(head_, &header, sizeof(header));
    return header;
  }

  void popFront()
  {
    const size_t size = front().recordSize();
    head_ = (head_ + size) % capacity();
    used_ -= size;
    --count_;
  }

  // The caller has made room: used() + header.recordSize() <= capacity().
  void push(const RecordHeader& header, const uint8_t* mask, const uint8_t* payload)
  {
    size_t pos = (head_ + used_) % capacity();
    pos = write(pos, &header, sizeof(header));
    pos = write(pos, mask, header.mask_size);
    write(pos, payload, header.payload_size);
    used_ += header.recordSize();
    ++count_;
  }

  // Copy the content, oldest record first, into `out` (at least used() bytes).
  void linearize(uint8_t* out) const { read(head_, out, used_); }

private:
  size_t write(size_t pos, const void* src, size_t size)
  {
    const auto* bytes = static_cast<const uint8_t*>(src);
    const size_t first = std::min(size, capacity() - pos);
    std::memcpy(buffer_.data() + pos, bytes, first);
    std::memcpy(buffer_.data(), bytes + first, size - first);
    return (pos + size) % capacity();
  }

  void read(size_t pos, void* dst, size_t size) const
  {
    auto* bytes = static_cast<uint8_t*>(dst);
    const size_t first = std::min(size, capacity() - pos);
    std::memcpy(bytes, buffer_.data() + pos, first);
    std::memcpy(bytes + first, buffer_.data(), size - first);
  }

  std::vector<uint8_t> buffer_;
  size_t head_ = 0;
  size_t used_ = 0;
  size_t count_ = 0;
};

}  // namespace

struct MCAPRingSink::Pimpl
{
  explicit Pimpl(MCAPRingOptions opt)
    : options(std::move(opt))
    , ring(options.capacity_bytes)
    , dump_buffer(options.capacity_bytes)
  {}

  // Ring side: touched by onSnapshot() and flushPendingDump().
  enum class Phase
  {
    Idle,        // no request triggered yet
    Collecting,  // triggered, waiting for the post-trigger interval
    Ready        // complete, waiting for the writer
  };

  const MCAPRingOptions options;
  std::atomic<uint64_t> request{ 0 };

  mutable std::mutex ring_mutex;
  ByteRing ring;
  Phase phase = Phase::Idle;
  nanoseconds trigger_time{ 0 }, dump_start{ 0 }, dump_end{ 0 };
  bool has_snapshot = false;
  nanoseconds last_timestamp{ 0 };
  uint64_t writer_busy_retries = 0;
  uint64_t evicted_by_capacity = 0;
  uint64_t dropped_oversize = 0;

  // Schemas, inserted by onSchema() and read by the writer thread. Entries are
  // never modified once inserted, so the writer uses them outside the lock.
  std::mutex schema_mutex;
  std::map<uint64_t, Schema> schemas;

  // Writer side. While writer_busy, the writer thread owns dump_buffer and job.
  mutable std::mutex writer_mutex;
  std::condition_variable writer_cv;
  bool writer_busy = false;
  bool writer_stop = false;
  std::vector<uint8_t> dump_buffer;
  size_t dump_size = 0;
  size_t dump_counter = 0;
  MCAPRingDump job;
  uint64_t dumps_written = 0;
  uint64_t dumps_failed = 0;
  std::function<void(const MCAPRingDump&)> callback;
  std::thread writer_thread;

  void store(const Snapshot& snapshot);
  bool tryHandOff(bool wait_for_writer);
  void writerLoop();
  void writeDump(MCAPRingDump& dump);
};

// Caller holds ring_mutex.
void MCAPRingSink::Pimpl::store(const Snapshot& snapshot)
{
  RecordHeader header{ snapshot.schema_hash, snapshot.timestamp.count(),
                       static_cast<uint32_t>(snapshot.active_mask.size()),
                       static_cast<uint32_t>(snapshot.payload.size()) };
  // Evict by age. While a dump is active, keep everything it may contain.
  nanoseconds horizon = saturatingSub(snapshot.timestamp, options.window);
  if(phase != Phase::Idle)
  {
    horizon = std::min(horizon, dump_start);
  }
  while(ring.count() > 0 && nanoseconds(ring.front().timestamp) < horizon)
  {
    ring.popFront();
  }

  const size_t size =
      sizeof(RecordHeader) + snapshot.active_mask.size() + snapshot.payload.size();
  if(size > ring.capacity() || snapshot.active_mask.size() > UINT32_MAX ||
     snapshot.payload.size() > UINT32_MAX)
  {
    ++dropped_oversize;
    return;
  }
  // Evict by capacity.
  while(ring.used() + size > ring.capacity())
  {
    ring.popFront();
    ++evicted_by_capacity;
  }
  ring.push(header, snapshot.active_mask.data(), snapshot.payload.data());
}

// Caller holds ring_mutex and phase == Ready. Copies the ring into the dump
// buffer and wakes the writer; returns false if the writer is busy and
// `wait_for_writer` is false.
bool MCAPRingSink::Pimpl::tryHandOff(bool wait_for_writer)
{
  {
    std::unique_lock lock(writer_mutex);
    if(writer_busy && wait_for_writer)
    {
      writer_cv.wait(lock, [this] { return !writer_busy; });
    }
    if(writer_busy)
    {
      ++writer_busy_retries;
      return false;
    }
    ring.linearize(dump_buffer.data());
    dump_size = ring.used();
    // clear() keeps the capacity: the worker thread allocates nothing here.
    job.path.clear();
    job.error.clear();
    job.trigger_time = trigger_time;
    job.start = dump_start;
    job.end = dump_end;
    job.messages = 0;
    job.ok = false;
    writer_busy = true;
  }
  writer_cv.notify_all();
  phase = Phase::Idle;
  request.store(0, std::memory_order_release);
  return true;
}

void MCAPRingSink::Pimpl::writerLoop()
{
  std::unique_lock lock(writer_mutex);
  while(true)
  {
    writer_cv.wait(lock, [this] { return writer_busy || writer_stop; });
    if(!writer_busy)
    {
      return;  // stop requested and nothing left to write
    }
    const auto notify = callback;
    lock.unlock();
    writeDump(job);
    if(notify)
    {
      try
      {
        notify(job);
      }
      catch(...)
      {
        // A throwing callback must not kill the writer thread.
      }
    }
    lock.lock();
    ++(job.ok ? dumps_written : dumps_failed);
    writer_busy = false;
    writer_cv.notify_all();
  }
}

// Runs on the writer thread, which owns dump_buffer and `dump`.
void MCAPRingSink::Pimpl::writeDump(MCAPRingDump& dump)
{
  dump.path = details::NumberedPath(options.filepath, ++dump_counter);
  try
  {
    mcap::McapWriter writer;
    mcap::McapWriterOptions writer_options(mcap_encoding::kEncoding);
    writer_options.compression =
        options.compression ? mcap::Compression::Zstd : mcap::Compression::None;
    auto status = writer.open(dump.path, writer_options);
    if(!status.ok())
    {
      throw std::runtime_error("failed to open MCAP file: " + status.message);
    }
    struct Channel
    {
      mcap::ChannelId id;
      uint32_t next_sequence;
    };
    std::unordered_map<uint64_t, Channel> channels;
    std::vector<uint8_t> scratch;
    for(size_t pos = 0; pos < dump_size;)
    {
      RecordHeader header;
      std::memcpy(&header, dump_buffer.data() + pos, sizeof(header));
      const uint8_t* mask = dump_buffer.data() + pos + sizeof(header);
      const uint8_t* payload = mask + header.mask_size;
      pos += header.recordSize();
      const nanoseconds timestamp(header.timestamp);
      if(timestamp < dump.start || timestamp > dump.end)
      {
        continue;
      }
      auto it = channels.find(header.schema_hash);
      if(it == channels.end())
      {
        const Schema* schema = nullptr;
        {
          std::scoped_lock lock(schema_mutex);
          auto found = schemas.find(header.schema_hash);
          schema = (found == schemas.end()) ? nullptr : &found->second;
        }
        if(!schema)
        {
          continue;  // cannot happen: onSchema() precedes the snapshots
        }
        it = channels
                 .emplace(header.schema_hash,
                          Channel{ mcap_encoding::AddChannel(writer, *schema), 1 })
                 .first;
      }
      status = mcap_encoding::WriteMessage(
          writer, it->second.id, it->second.next_sequence++, timestamp,
          { mask, header.mask_size }, { payload, header.payload_size }, scratch);
      if(!status.ok())
      {
        throw std::runtime_error("MCAP write failed: " + status.message);
      }
      ++dump.messages;
    }
    writer.close();
    dump.ok = true;
  }
  catch(const std::exception& e)
  {
    dump.error = e.what();
  }
}

//--------------------------------------------------

MCAPRingSink::MCAPRingSink(MCAPRingOptions options)
{
  if(options.capacity_bytes == 0)
  {
    throw std::invalid_argument("MCAPRingSink: capacity_bytes must be > 0");
  }
  if(options.window.count() < 0)
  {
    throw std::invalid_argument("MCAPRingSink: window must not be negative");
  }
  _p = std::make_unique<Pimpl>(std::move(options));
  _p->writer_thread = std::thread([this] { _p->writerLoop(); });
}

MCAPRingSink::~MCAPRingSink()
{
  {
    std::scoped_lock lock(_p->writer_mutex);
    _p->writer_stop = true;
  }
  _p->writer_cv.notify_all();
  _p->writer_thread.join();
}

bool MCAPRingSink::requestDump(std::chrono::nanoseconds post_trigger)
{
  const auto delay =
      static_cast<uint64_t>(std::max<nanoseconds::rep>(post_trigger.count(), 0));
  uint64_t expected = 0;
  return _p->request.compare_exchange_strong(
      expected, kPending | std::min(delay, kMaxDelay), std::memory_order_acq_rel,
      std::memory_order_relaxed);
}

bool MCAPRingSink::dumpRequested() const
{
  return _p->request.load(std::memory_order_acquire) != 0;
}

void MCAPRingSink::onSchema(const Schema& schema)
{
  std::scoped_lock lock(_p->schema_mutex);
  _p->schemas.try_emplace(schema.hash, schema);
}

void MCAPRingSink::onSnapshot(const SnapshotRef& ref)
{
  const Snapshot& snapshot = *ref;
  auto& p = *_p;
  std::scoped_lock lock(p.ring_mutex);
  p.has_snapshot = true;
  p.last_timestamp = snapshot.timestamp;

  if(p.phase == Pimpl::Phase::Idle)
  {
    const uint64_t word = p.request.load(std::memory_order_acquire);
    if(word & kPending)
    {
      p.phase = Pimpl::Phase::Collecting;
      p.trigger_time = snapshot.timestamp;
      p.dump_start = saturatingSub(snapshot.timestamp, p.options.window);
      p.dump_end = saturatingAdd(snapshot.timestamp, nanoseconds(word & kMaxDelay));
    }
  }
  p.store(snapshot);
  if(p.phase == Pimpl::Phase::Collecting && snapshot.timestamp >= p.dump_end)
  {
    p.phase = Pimpl::Phase::Ready;
  }
  if(p.phase == Pimpl::Phase::Ready)
  {
    p.tryHandOff(false);  // writer busy: retried at the next snapshot
  }
}

bool MCAPRingSink::flushPendingDump()
{
  auto& p = *_p;
  {
    std::unique_lock lock(p.ring_mutex);
    if(p.phase == Pimpl::Phase::Idle)
    {
      if(!p.has_snapshot || !(p.request.load(std::memory_order_acquire) & kPending))
      {
        lock.unlock();
        waitForWriter();  // a dump handed off earlier may still be in progress
        return false;
      }
      p.trigger_time = p.last_timestamp;
      p.dump_start = saturatingSub(p.last_timestamp, p.options.window);
      p.dump_end = p.last_timestamp;
    }
    p.dump_end = std::min(p.dump_end, p.last_timestamp);
    p.phase = Pimpl::Phase::Ready;
    p.tryHandOff(true);
  }
  waitForWriter();
  return true;
}

void MCAPRingSink::waitForWriter()
{
  std::unique_lock lock(_p->writer_mutex);
  _p->writer_cv.wait(lock, [this] { return !_p->writer_busy; });
}

void MCAPRingSink::setDumpCallback(std::function<void(const MCAPRingDump&)> callback)
{
  std::scoped_lock lock(_p->writer_mutex);
  _p->callback = std::move(callback);
}

MCAPRingStats MCAPRingSink::stats() const
{
  MCAPRingStats stats;
  {
    std::scoped_lock lock(_p->ring_mutex);
    stats.writer_busy_retries = _p->writer_busy_retries;
    stats.evicted_by_capacity = _p->evicted_by_capacity;
    stats.dropped_oversize = _p->dropped_oversize;
    stats.stored_snapshots = _p->ring.count();
    stats.stored_bytes = _p->ring.used();
  }
  std::scoped_lock lock(_p->writer_mutex);
  stats.dumps_written = _p->dumps_written;
  stats.dumps_failed = _p->dumps_failed;
  return stats;
}

}  // namespace DataTamer
