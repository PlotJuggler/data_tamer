#pragma once

#include "data_tamer/data_sink.hpp"
#include "data_tamer/sinks/mcap_sink.hpp"  // details::NumberedPath

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace DataTamer
{

/// Configuration of an MCAPRingSink.
struct MCAPRingOptions
{
  /// Dump number N (1, 2, 3, ...) is written to details::NumberedPath(filepath, N):
  /// "crash.mcap" -> "crash_1.mcap", "crash_2.mcap", ... The counter restarts
  /// with every sink, so existing files of an earlier run are overwritten: use
  /// a different filepath per run (a timestamp, for instance) to keep them.
  std::string filepath = "flight_recorder.mcap";
  /// How much history before the trigger a dump contains, in snapshot time.
  std::chrono::nanoseconds window = std::chrono::seconds(10);
  /// Size of the RAM ring. A second buffer of the same size holds the dump
  /// being written, so the sink uses about 2 * capacity_bytes. When the ring
  /// is full the oldest snapshots are evicted, even if younger than `window`.
  size_t capacity_bytes = size_t(64) * 1024 * 1024;
  /// Compress the dump files with zstd.
  bool compression = false;
};

/// Outcome of one dump, passed to the MCAPRingSink dump callback.
struct MCAPRingDump
{
  std::string path;
  /// Timestamp of the snapshot that triggered the dump.
  std::chrono::nanoseconds trigger_time{ 0 };
  /// The dump holds the stored snapshots whose timestamp is in [start, end].
  std::chrono::nanoseconds start{ 0 };
  std::chrono::nanoseconds end{ 0 };
  size_t messages = 0;
  bool ok = false;
  std::string error;
};

struct MCAPRingStats
{
  uint64_t dumps_written = 0;
  uint64_t dumps_failed = 0;
  /// Finished dumps that found the writer busy and waited for a later snapshot.
  uint64_t writer_busy_retries = 0;
  /// Snapshots evicted from the ring because it was full (not because of age).
  uint64_t evicted_by_capacity = 0;
  /// Snapshots larger than the whole ring, never stored.
  uint64_t dropped_oversize = 0;
  /// Current content of the ring.
  size_t stored_snapshots = 0;
  size_t stored_bytes = 0;
};

/**
 * @brief Flight recorder: keeps the last `window` of every channel attached to
 * it in a preallocated RAM ring and writes an MCAP file only when asked to,
 * with requestDump(), for example on a protective stop or a fault.
 *
 *   MCAPRingOptions options;
 *   options.filepath = "fault.mcap";
 *   options.window = std::chrono::seconds(5);
 *   auto worker = MCAPRingSink::create(options);
 *   channel->addDataSink(worker);
 *   auto& recorder = worker->as<MCAPRingSink>();
 *   ...
 *   recorder.requestDump(std::chrono::seconds(2));  // from any thread, RT-safe
 *
 * Trigger and content. The trigger is the first snapshot delivered to the sink
 * after the request, and all times are snapshot timestamps, so a dump behaves
 * the same in simulation, replay and on hardware. With trigger time T, the
 * dump contains the stored snapshots of every channel with a timestamp in
 * [T - window, T + post_trigger], and the MCAP schema and channel records of
 * the channels that appear in it. It is complete at the first snapshot with a
 * timestamp >= T + post_trigger.
 *
 * Real time. onSnapshot() copies mask and payload into the ring and releases
 * the pool slot at once; after the ring is allocated it allocates nothing.
 * A finished dump is copied into the second buffer and written to disk by a
 * writer thread of the sink, so the SinkWorker never waits for file I/O. If
 * the writer is still busy with the previous dump, the hand-off is retried at
 * the next snapshot, and in the meantime the ring keeps the dumped interval.
 *
 * Shutdown. A request made just before shutdown has no snapshot left to
 * trigger it: call flushPendingDump() after SinkWorker::stop(). The destructor
 * finishes the file being written but does not start a pending dump.
 */
class MCAPRingSink : public DataSink
{
public:
  explicit MCAPRingSink(MCAPRingOptions options);
  ~MCAPRingSink() override;
  MCAPRingSink(const MCAPRingSink&) = delete;
  MCAPRingSink& operator=(const MCAPRingSink&) = delete;

  /// Ready-to-attach sink: SinkWorker::create<MCAPRingSink>(options).
  static std::shared_ptr<SinkWorker> create(MCAPRingOptions options)
  {
    return SinkWorker::create<MCAPRingSink>(std::move(options));
  }

  /**
   * @brief Ask for a dump that also covers `post_trigger` after the trigger.
   * Lock-free and allocation-free (a single compare-exchange): call it from any
   * thread, real-time ones included.
   *
   * Returns false, and the request is ignored, while an earlier request is still
   * active: from its requestDump() until its dump has been handed to the writer
   * thread. Requests are not coalesced: an ignored request does not extend the
   * active dump. Writing the file to disk does not block new requests.
   */
  bool requestDump(std::chrono::nanoseconds post_trigger = std::chrono::nanoseconds(0));

  /// True from an accepted requestDump() until its dump is handed to the writer.
  [[nodiscard]] bool dumpRequested() const;

  /**
   * @brief Write the active request, if any, with what the ring holds now,
   * and wait until it is on disk. A request no snapshot has triggered yet uses
   * the latest stored snapshot as trigger; a dump still waiting for its
   * post-trigger interval is cut at the latest snapshot. Call it after
   * SinkWorker::stop() (or between drain() calls with manual delivery).
   * Returns true if it wrote a dump (see MCAPRingDump::ok for the outcome).
   * In every case it returns after the writer has finished all dumps handed
   * to it so far.
   */
  bool flushPendingDump();

  /// Wait until the writer thread has no dump in progress.
  void waitForWriter();

  /// Called on the writer thread after each dump, successful or not.
  void setDumpCallback(std::function<void(const MCAPRingDump&)> callback);

  [[nodiscard]] MCAPRingStats stats() const;

protected:
  void onSchema(const Schema& schema) override;
  void onSnapshot(const SnapshotRef& snapshot) override;

private:
  struct Pimpl;
  std::unique_ptr<Pimpl> _p;
};

}  // namespace DataTamer
