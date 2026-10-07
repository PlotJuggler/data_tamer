#include "data_tamer/data_tamer.hpp"
#include "data_tamer/channel.hpp"
#include "data_tamer/data_sink.hpp"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace DataTamer
{

namespace
{
// Throws what the LogChannel setters throw, before the channel is prepared.
void ApplyDefaults(LogChannel& channel, const ChannelDefaults& defaults)
{
  const bool in_time = defaults.pool_stall_tolerance.count() != 0 ||
                       defaults.pool_snapshot_period.count() != 0;
  if(in_time && defaults.pool_capacity != 0)
  {
    throw std::invalid_argument("ChannelDefaults: set the pool capacity as a count or "
                                "in time, not both");
  }
  if(in_time)
  {
    channel.setPoolCapacity(defaults.pool_stall_tolerance, defaults.pool_snapshot_period);
  }
  else if(defaults.pool_capacity != 0)
  {
    channel.setPoolCapacity(defaults.pool_capacity);
  }
  if(defaults.payload_capacity != 0)
  {
    channel.setPayloadCapacity(defaults.payload_capacity);
  }
}
}  // namespace

struct ChannelsRegistry::Pimpl
{
  std::unordered_map<std::string, std::shared_ptr<LogChannel>> channels;
  std::unordered_set<std::shared_ptr<SinkWorker>> default_sinks;
  ChannelDefaults channel_defaults;
  std::mutex mutex;
};

ChannelsRegistry::ChannelsRegistry() : _p(new Pimpl) {}

ChannelsRegistry::~ChannelsRegistry() {}

ChannelsRegistry& ChannelsRegistry::Global()
{
  static ChannelsRegistry obj;
  return obj;
}

void ChannelsRegistry::addDefaultSink(std::shared_ptr<SinkWorker> sink)
{
  if(!sink)
  {
    throw std::invalid_argument("addDefaultSink: null sink");
  }
  std::scoped_lock lk(_p->mutex);
  const bool inserted = _p->default_sinks.insert(sink).second;
  std::vector<LogChannel*> attached;
  try
  {
    for(const auto& [name, channel] : _p->channels)
    {
      const auto held = channel->dataSinks();
      if(std::find(held.begin(), held.end(), sink) == held.end())
      {
        channel->addDataSink(sink);
        attached.push_back(channel.get());
      }
    }
  }
  catch(...)
  {
    for(LogChannel* channel : attached)
    {
      channel->removeDataSink(sink);
    }
    if(inserted)
    {
      _p->default_sinks.erase(sink);
    }
    throw;
  }
}

void ChannelsRegistry::setChannelDefaults(const ChannelDefaults& defaults)
{
  // Validated on a scratch channel, so that it throws exactly what the
  // LogChannel setters would throw later, in getChannel().
  ApplyDefaults(*LogChannel::create("channel_defaults"), defaults);
  std::scoped_lock lk(_p->mutex);
  _p->channel_defaults = defaults;
}

std::shared_ptr<LogChannel> ChannelsRegistry::getChannel(std::string const& channel_name)
{
  std::scoped_lock lk(_p->mutex);
  auto it = _p->channels.find(channel_name);
  if(it == _p->channels.end())
  {
    auto new_channel = LogChannel::create(channel_name);
    ApplyDefaults(*new_channel, _p->channel_defaults);
    for(auto const& sink : _p->default_sinks)
    {
      new_channel->addDataSink(sink);
    }
    _p->channels.insert({ channel_name, new_channel });
    return new_channel;
  }
  return it->second;
}

void ChannelsRegistry::stopAll()
{
  std::vector<std::shared_ptr<SinkWorker>> sinks;
  {
    std::scoped_lock lk(_p->mutex);
    sinks.assign(_p->default_sinks.begin(), _p->default_sinks.end());
    for(const auto& [name, channel] : _p->channels)
    {
      for(auto& sink : channel->dataSinks())
      {
        sinks.push_back(std::move(sink));
      }
    }
  }
  std::sort(sinks.begin(), sinks.end());
  sinks.erase(std::unique(sinks.begin(), sinks.end()), sinks.end());
  // stop() delivers, joins and runs onStop(): never under the registry lock.
  for(const auto& sink : sinks)
  {
    sink->stop();
  }
}

void ChannelsRegistry::clear()
{
  // Destroying channels and sinks can block (they drain and join): never do
  // that while holding the registry lock.
  decltype(_p->channels) channels;
  decltype(_p->default_sinks) sinks;
  {
    std::scoped_lock lk(_p->mutex);
    channels.swap(_p->channels);
    sinks.swap(_p->default_sinks);
    _p->channel_defaults = ChannelDefaults{};
  }
}

}  // namespace DataTamer
