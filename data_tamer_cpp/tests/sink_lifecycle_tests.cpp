// DataSink::onStop(), ChannelsRegistry::stopAll(), addDefaultSink() on existing
// channels and ChannelsRegistry::setChannelDefaults().
#include "data_tamer/channel.hpp"
#include "data_tamer/data_tamer.hpp"
#include "data_tamer/sinks/mcap_sink.hpp"
#include "test_sinks.hpp"

#include <gtest/gtest.h>
#include <mcap/reader.hpp>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace DataTamer;
using Delivery = SinkWorker::Delivery;
using std::chrono::milliseconds;
using std::chrono::nanoseconds;

namespace
{
// What a LifecycleSink saw. Shared with the test so that it survives the sink.
struct Journal
{
  mutable std::mutex mutex;
  std::unordered_map<std::string, int> schemas;  // channel name -> onSchema() calls
  int snapshots = 0;
  int stops = 0;
  int snapshots_at_stop = -1;
  std::thread::id stop_thread;
  bool snapshot_in_progress_at_stop = false;
  bool throw_on_stop = false;

  // Optional gate: the first onSnapshot() announces itself, then waits for release.
  bool gate_first_snapshot = false;
  bool entered = false;
  bool released = false;
  bool in_snapshot = false;
  std::condition_variable cv;

  int stopCount() const
  {
    std::lock_guard lock(mutex);
    return stops;
  }
  int snapshotCount() const
  {
    std::lock_guard lock(mutex);
    return snapshots;
  }
  int schemaCount(const std::string& channel) const
  {
    std::lock_guard lock(mutex);
    const auto it = schemas.find(channel);
    return it == schemas.end() ? 0 : it->second;
  }
};

class LifecycleSink : public DataSink
{
public:
  explicit LifecycleSink(std::shared_ptr<Journal> journal) : journal_(std::move(journal))
  {}

protected:
  void onSchema(const Schema& schema) override
  {
    std::lock_guard lock(journal_->mutex);
    ++journal_->schemas[schema.channel_name];
  }

  void onSnapshot(const SnapshotRef&) override
  {
    std::unique_lock lock(journal_->mutex);
    journal_->in_snapshot = true;
    if(journal_->gate_first_snapshot && !journal_->entered)
    {
      journal_->entered = true;
      journal_->cv.notify_all();
      journal_->cv.wait(lock, [this] { return journal_->released; });
    }
    ++journal_->snapshots;
    journal_->in_snapshot = false;
  }

  void onStop() override
  {
    std::lock_guard lock(journal_->mutex);
    ++journal_->stops;
    journal_->snapshots_at_stop = journal_->snapshots;
    journal_->stop_thread = std::this_thread::get_id();
    journal_->snapshot_in_progress_at_stop = journal_->in_snapshot;
    if(journal_->throw_on_stop)
    {
      throw std::runtime_error("stop failed");
    }
  }

private:
  std::shared_ptr<Journal> journal_;
};

std::shared_ptr<SinkWorker> lifecycleWorker(const std::shared_ptr<Journal>& journal,
                                            Delivery delivery = Delivery::Manual)
{
  return std::make_shared<SinkWorker>(std::make_unique<LifecycleSink>(journal), delivery);
}

std::string tempPath(const std::string& tag)
{
  return (std::filesystem::temp_directory_path() /
          ("data_tamer_lifecycle_" + tag + "_" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
           ".mcap"))
      .string();
}

// Snapshots accepted until the pool runs out; nothing is delivered meanwhile
// (manual sink, no drain), so this is the pool capacity.
size_t acceptedUntilPoolExhausted(LogChannel& channel)
{
  size_t accepted = 0;
  while(true)
  {
    const auto result = channel.tryTakeSnapshot();
    if(result != SnapshotResult::ok)
    {
      EXPECT_EQ(result, SnapshotResult::pool_exhausted);
      return accepted;
    }
    ++accepted;
  }
}
}  // namespace

//------------------------------------------------------------------------------
// DataSink::onStop()

TEST(SinkLifecycle, StopDeliversEverythingThenCallsOnStopOnceOnTheStoppingThread)
{
  for(const auto delivery : { Delivery::Threaded, Delivery::Manual })
  {
    auto journal = std::make_shared<Journal>();
    auto worker = lifecycleWorker(journal, delivery);
    double value = 1.0;
    auto channel = LogChannel::create("lifecycle");
    channel->registerValue("value", &value);
    channel->addDataSink(worker);
    for(int i = 0; i < 10; ++i)
    {
      ASSERT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
    }
    worker->stop();
    {
      std::lock_guard lock(journal->mutex);
      EXPECT_EQ(journal->stops, 1);
      EXPECT_EQ(journal->snapshots_at_stop, 10) << "onStop() after the last delivery";
      EXPECT_EQ(journal->stop_thread, std::this_thread::get_id());
    }
    EXPECT_EQ(channel->takeSnapshot(), SnapshotResult::rejected);

    worker->stop();  // idempotent
    EXPECT_EQ(journal->stopCount(), 1);

    // Each start()/stop() cycle finishes the sink again.
    worker->start();
    for(int i = 0; i < 5; ++i)
    {
      ASSERT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
    }
    worker->stop();
    {
      std::lock_guard lock(journal->mutex);
      EXPECT_EQ(journal->stops, 2);
      EXPECT_EQ(journal->snapshots_at_stop, 15);
    }
    channel.reset();
    worker.reset();  // stopped already: the destructor does not call onStop()
    EXPECT_EQ(journal->stopCount(), 2);
    EXPECT_EQ(journal->snapshotCount(), 15);
  }
}

TEST(SinkLifecycle, DestroyingARunningWorkerFinishesTheSink)
{
  for(const auto delivery : { Delivery::Threaded, Delivery::Manual })
  {
    auto journal = std::make_shared<Journal>();
    {
      auto worker = lifecycleWorker(journal, delivery);
      double value = 1.0;
      auto channel = LogChannel::create("lifecycle_destroy");
      channel->registerValue("value", &value);
      channel->addDataSink(worker);
      for(int i = 0; i < 3; ++i)
      {
        ASSERT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
      }
    }  // channel first, then the last reference to the worker
    std::lock_guard lock(journal->mutex);
    EXPECT_EQ(journal->stops, 1);
    EXPECT_EQ(journal->snapshots_at_stop, 3);
  }
}

TEST(SinkLifecycle, OnStopThrowIsCountedAndDoesNotEscape)
{
  auto journal = std::make_shared<Journal>();
  journal->throw_on_stop = true;
  auto worker = lifecycleWorker(journal);
  EXPECT_NO_THROW(worker->stop());
  EXPECT_EQ(worker->errors(), 1u);
  EXPECT_EQ(worker->lastError(), "stop failed");
  worker->start();
  EXPECT_NO_THROW(worker->stop());
  EXPECT_EQ(worker->errors(), 2u);
  EXPECT_EQ(journal->stopCount(), 2);
  EXPECT_NO_THROW(worker.reset());
  EXPECT_EQ(journal->stopCount(), 2);
}

// onStop() is serialized with onSnapshot(): a stop() racing a callback in
// progress waits for it, delivers what is queued behind it, then finishes.
TEST(SinkLifecycle, OnStopWaitsForTheCallbackInProgressAndTheQueue)
{
  auto journal = std::make_shared<Journal>();
  journal->gate_first_snapshot = true;
  auto worker = lifecycleWorker(journal, Delivery::Threaded);
  double value = 1.0;
  auto channel = LogChannel::create("lifecycle_race");
  channel->registerValue("value", &value);
  channel->addDataSink(worker);
  ASSERT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
  {
    std::unique_lock lock(journal->mutex);
    ASSERT_TRUE(journal->cv.wait_for(lock, std::chrono::seconds(5),
                                     [&] { return journal->entered; }));
  }
  int accepted = 1;
  ASSERT_EQ(channel->takeSnapshot(), SnapshotResult::ok);  // queued behind the gate
  ++accepted;
  std::thread stopper([&] { worker->stop(); });
  // Admitted until stop() closes admission: then the stopper is inside stop().
  while(channel->takeSnapshot() == SnapshotResult::ok)
  {
    ++accepted;
  }
  EXPECT_EQ(journal->stopCount(), 0) << "onStop() ran during onSnapshot()";
  {
    std::lock_guard lock(journal->mutex);
    journal->released = true;
  }
  journal->cv.notify_all();
  const auto stopper_id = stopper.get_id();
  stopper.join();
  std::lock_guard lock(journal->mutex);
  EXPECT_EQ(journal->stops, 1);
  EXPECT_FALSE(journal->snapshot_in_progress_at_stop);
  EXPECT_EQ(journal->snapshots_at_stop, accepted);
  EXPECT_EQ(journal->stop_thread, stopper_id);
}

// SinkWorker::stop() alone yields a complete, finalized MCAP file (summary and
// footer written): no stopRecording() needed.
TEST(SinkLifecycle, McapStopFinalizesTheFileAndRestartRecordsAgain)
{
  const auto path = tempPath("mcap");
  const auto second_path = tempPath("mcap_second");
  auto worker = MCAPSink::create(path);
  double value = 1.0;
  auto channel = LogChannel::create("lifecycle_mcap");
  channel->registerValue("value", &value);
  channel->addDataSink(worker);
  for(int i = 0; i < 7; ++i)
  {
    ASSERT_EQ(channel->takeSnapshot(nanoseconds(i + 1)), SnapshotResult::ok);
  }
  worker->stop();
  EXPECT_EQ(worker->errors(), 0u);

  const auto count_messages = [](const std::string& file) {
    mcap::McapReader reader;
    EXPECT_TRUE(reader.open(file).ok()) << file;
    // Fails on a file whose writer was not closed: no footer, no summary.
    EXPECT_TRUE(reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan).ok()) << file;
    size_t count = 0;
    for(const auto& message : reader.readMessages())
    {
      (void)message;
      ++count;
    }
    reader.close();
    return count;
  };
  EXPECT_EQ(count_messages(path), 7u);

  // Recording again after stop(): restartRecording(), then start().
  worker->as<MCAPSink>().restartRecording(second_path);
  worker->start();
  for(int i = 0; i < 4; ++i)
  {
    ASSERT_EQ(channel->takeSnapshot(nanoseconds(100 + i)), SnapshotResult::ok);
  }
  channel.reset();
  worker.reset();  // the destructor stops, which finalizes the second file
  EXPECT_EQ(count_messages(second_path), 4u);
  EXPECT_EQ(count_messages(path), 7u) << "the first file is left alone";
  std::filesystem::remove(path);
  std::filesystem::remove(second_path);
}

//------------------------------------------------------------------------------
// ChannelsRegistry

TEST(ChannelsRegistry, AddDefaultSinkReachesExistingChannels)
{
  ChannelsRegistry registry;
  double value = 1.0;
  auto other_journal = std::make_shared<Journal>();
  auto other = lifecycleWorker(other_journal);

  auto open_channel = registry.getChannel("open");
  open_channel->registerValue("value", &value);
  auto prepared_channel = registry.getChannel("prepared");
  prepared_channel->registerValue("value", &value);
  prepared_channel->addDataSink(other);
  prepared_channel->prepare();

  auto journal = std::make_shared<Journal>();
  auto sink = lifecycleWorker(journal);
  registry.addDefaultSink(sink);
  // The prepared channel announced its schema at once; the open one will at prepare().
  EXPECT_EQ(journal->schemaCount("prepared"), 1);
  EXPECT_EQ(journal->schemaCount("open"), 0);
  EXPECT_EQ(open_channel->getNumberOfSinks(), 1u);
  EXPECT_EQ(prepared_channel->getNumberOfSinks(), 2u);

  auto later = registry.getChannel("later");
  later->registerValue("value", &value);
  EXPECT_EQ(later->getNumberOfSinks(), 1u);

  ASSERT_EQ(open_channel->takeSnapshot(), SnapshotResult::ok);
  ASSERT_EQ(prepared_channel->tryTakeSnapshot(), SnapshotResult::ok);
  ASSERT_EQ(later->takeSnapshot(), SnapshotResult::ok);
  sink->drain();
  EXPECT_EQ(journal->schemaCount("open"), 1);
  EXPECT_EQ(journal->schemaCount("prepared"), 1) << "announced once";
  EXPECT_EQ(journal->schemaCount("later"), 1);
  EXPECT_EQ(journal->snapshotCount(), 3);

  // Adding it again changes nothing.
  registry.addDefaultSink(sink);
  EXPECT_EQ(prepared_channel->getNumberOfSinks(), 2u);
  EXPECT_EQ(journal->schemaCount("prepared"), 1);
}

TEST(ChannelsRegistry, AddDefaultSinkIsUndoneWhenAChannelRefusesIt)
{
  ChannelsRegistry registry;
  auto shared_journal = std::make_shared<Journal>();
  auto shared = lifecycleWorker(shared_journal);

  auto full = registry.getChannel("full");
  std::vector<std::shared_ptr<SinkWorker>> fillers;
  for(int i = 0; i < 8; ++i)  // the most a channel holds
  {
    fillers.push_back(lifecycleWorker(std::make_shared<Journal>()));
    full->addDataSink(fillers.back());
  }
  auto holder = registry.getChannel("holder");
  holder->addDataSink(shared);  // already attached: must survive the undo
  auto plain = registry.getChannel("plain");
  auto plain2 = registry.getChannel("plain2");

  EXPECT_THROW(registry.addDefaultSink(shared), std::runtime_error);
  EXPECT_EQ(full->getNumberOfSinks(), 8u);
  EXPECT_EQ(holder->getNumberOfSinks(), 1u);
  EXPECT_EQ(plain->getNumberOfSinks(), 0u);
  EXPECT_EQ(plain2->getNumberOfSinks(), 0u);
  // Not a default sink either.
  EXPECT_EQ(registry.getChannel("after")->getNumberOfSinks(), 0u);
}

TEST(ChannelsRegistry, ChannelDefaultsApplyToNewChannelsOnly)
{
  ChannelsRegistry registry;
  double value = 1.0;
  auto existing = registry.getChannel("existing");

  ChannelDefaults defaults;
  defaults.pool_capacity = 3;
  registry.setChannelDefaults(defaults);

  auto sink = lifecycleWorker(std::make_shared<Journal>());  // never drained
  registry.addDefaultSink(sink);
  auto created = registry.getChannel("created");
  EXPECT_EQ(registry.getChannel("created"), created) << "applied once, at creation";
  for(const auto& channel : { existing, created })
  {
    channel->registerValue("value", &value);
    channel->prepare();
  }
  EXPECT_EQ(acceptedUntilPoolExhausted(*created), 3u);
  // An existing channel keeps the library default.
  EXPECT_EQ(acceptedUntilPoolExhausted(*existing), 64u);

  // The pool in time: ceil(10 ms / 3 ms) = 4 slots.
  defaults = {};
  defaults.pool_stall_tolerance = milliseconds(10);
  defaults.pool_snapshot_period = milliseconds(3);
  registry.setChannelDefaults(defaults);
  auto timed = registry.getChannel("timed");
  timed->registerValue("value", &value);
  timed->prepare();
  EXPECT_EQ(acceptedUntilPoolExhausted(*timed), 4u);

  // clear() forgets the defaults with the channels.
  registry.clear();
  auto fresh = registry.getChannel("fresh");
  fresh->registerValue("value", &value);
  fresh->addDataSink(sink);
  fresh->prepare();
  EXPECT_EQ(acceptedUntilPoolExhausted(*fresh), 64u);
}

TEST(ChannelsRegistry, ChannelDefaultsPayloadCapacityIsAFloor)
{
  // A vector that grows after prepare(): tryTakeSnapshot() refuses the snapshot
  // when it outgrows the slot, unless the defaults reserved enough.
  for(const size_t floor : { size_t{ 0 }, size_t{ 4096 } })
  {
    ChannelsRegistry registry;
    ChannelDefaults defaults;
    defaults.payload_capacity = floor;
    registry.setChannelDefaults(defaults);
    registry.addDefaultSink(lifecycleWorker(std::make_shared<Journal>()));
    auto channel = registry.getChannel("payload");
    std::vector<double> values(1, 0.0);
    channel->registerValue("values", &values);
    channel->prepare();
    values.resize(400);  // 3200 bytes, beyond the automatic 256-byte reserve
    EXPECT_EQ(channel->tryTakeSnapshot(),
              floor == 0 ? SnapshotResult::oversize : SnapshotResult::ok)
        << "payload floor " << floor;
  }
}

TEST(ChannelsRegistry, ChannelDefaultsAreValidatedWhenSet)
{
  ChannelsRegistry registry;
  ChannelDefaults good;
  good.pool_capacity = 5;
  registry.setChannelDefaults(good);

  ChannelDefaults both = good;
  both.pool_stall_tolerance = milliseconds(10);
  both.pool_snapshot_period = milliseconds(1);
  EXPECT_THROW(registry.setChannelDefaults(both), std::invalid_argument);

  ChannelDefaults half;
  half.pool_stall_tolerance = milliseconds(10);
  EXPECT_THROW(registry.setChannelDefaults(half), std::invalid_argument);

  ChannelDefaults negative;
  negative.pool_stall_tolerance = milliseconds(-10);
  negative.pool_snapshot_period = milliseconds(1);
  EXPECT_THROW(registry.setChannelDefaults(negative), std::invalid_argument);

  ChannelDefaults huge;
  huge.pool_capacity = std::numeric_limits<size_t>::max();
  EXPECT_THROW(registry.setChannelDefaults(huge), std::length_error);

  // A refused call keeps the previous defaults.
  double value = 1.0;
  registry.addDefaultSink(lifecycleWorker(std::make_shared<Journal>()));
  auto channel = registry.getChannel("validated");
  channel->registerValue("value", &value);
  channel->prepare();
  EXPECT_EQ(acceptedUntilPoolExhausted(*channel), 5u);
}

TEST(ChannelsRegistry, StopAllStopsEverySinkItReachesOnce)
{
  ChannelsRegistry registry;
  double value = 1.0;
  auto default_journal = std::make_shared<Journal>();
  auto direct_journal = std::make_shared<Journal>();
  auto outside_journal = std::make_shared<Journal>();
  auto default_sink = lifecycleWorker(default_journal, Delivery::Threaded);
  auto direct_sink = lifecycleWorker(direct_journal, Delivery::Threaded);
  auto outside_sink = lifecycleWorker(outside_journal, Delivery::Threaded);

  registry.addDefaultSink(default_sink);
  auto first = registry.getChannel("first");
  auto second = registry.getChannel("second");
  // One sink attached directly to two registry channels.
  first->addDataSink(direct_sink);
  second->addDataSink(direct_sink);
  // A channel the registry does not know.
  auto outside = LogChannel::create("outside");
  outside->addDataSink(outside_sink);
  for(const auto& channel : { first, second, outside })
  {
    channel->registerValue("value", &value);
    for(int i = 0; i < 5; ++i)
    {
      ASSERT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
    }
  }

  registry.stopAll();
  {
    std::lock_guard lock(default_journal->mutex);
    EXPECT_EQ(default_journal->stops, 1);
    EXPECT_EQ(default_journal->snapshots_at_stop, 10);
    EXPECT_EQ(default_journal->stop_thread, std::this_thread::get_id());
  }
  {
    std::lock_guard lock(direct_journal->mutex);
    EXPECT_EQ(direct_journal->stops, 1);
    EXPECT_EQ(direct_journal->snapshots_at_stop, 10);
  }
  EXPECT_EQ(outside_journal->stopCount(), 0);
  EXPECT_EQ(first->takeSnapshot(), SnapshotResult::rejected);
  EXPECT_EQ(second->takeSnapshot(), SnapshotResult::rejected);
  EXPECT_EQ(outside->takeSnapshot(), SnapshotResult::ok);
  EXPECT_EQ(first->getNumberOfSinks(), 2u) << "sinks stay attached";

  registry.stopAll();  // idempotent
  EXPECT_EQ(default_journal->stopCount(), 1);
  EXPECT_EQ(direct_journal->stopCount(), 1);

  // A sink restarted on its own is stopped (and finished) again.
  direct_sink->start();
  ASSERT_EQ(first->takeSnapshot(), SnapshotResult::partial);  // default sink stopped
  registry.stopAll();
  EXPECT_EQ(direct_journal->stopCount(), 2);
  EXPECT_EQ(default_journal->stopCount(), 1);
}
