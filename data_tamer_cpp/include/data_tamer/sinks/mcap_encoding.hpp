#pragma once

#include "data_tamer/data_sink.hpp"
#include "data_tamer/types.hpp"

#include <mcap/writer.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

/**
 * The MCAP encoding of docs/wire_format.md (section 4.1), shared by MCAPSink and
 * by any other code that writes data_tamer MCAP files, for example a sink that
 * stores snapshots and writes them later. Header-only: it uses the mcap library
 * of the including translation unit, so the caller must link MCAP itself.
 *
 *   mcap::McapWriter writer;
 *   writer.open(path, mcap::McapWriterOptions(mcap_encoding::kEncoding));
 *   const auto id = mcap_encoding::addChannel(writer, schema);
 *   std::vector<uint8_t> scratch;  // reused: no allocation once large enough
 *   uint32_t sequence = 1;
 *   mcap_encoding::writeSnapshot(writer, id, sequence++, snapshot, scratch);
 *
 * Sequence numbers are provided by the caller: MCAPSink counts 1, 2, 3, ... per
 * MCAP channel and file, and a writer of stored data should do the same (or
 * keep the numbers it recorded), so that readers can detect gaps.
 */
namespace DataTamer::mcap_encoding
{

/// Writer profile, schema encoding and message encoding.
inline constexpr char kEncoding[] = "data_tamer";

/// Non-owning view of bytes (std::span is C++20; the public headers are C++17).
struct ByteSpan
{
  const uint8_t* data = nullptr;
  size_t size = 0;

  ByteSpan() = default;
  ByteSpan(const uint8_t* ptr, size_t count) : data(ptr), size(count) {}
  ByteSpan(const std::vector<uint8_t>& v) : data(v.data()), size(v.size()) {}
};

/// Name of the MCAP schema record of `schema`: "<channel_name>::<hash>".
inline std::string schemaName(const Schema& schema)
{
  return schema.channel_name + "::" + std::to_string(schema.hash);
}

/// Register the MCAP schema and channel records of `schema` and return the
/// channel id to pass to writeMessage()/writeSnapshot(). Call it once per schema
/// and file (MCAP ids are per file).
inline mcap::ChannelId addChannel(mcap::McapWriter& writer, const Schema& schema)
{
  std::ostringstream ss;
  ss << schema;
  mcap::Schema mcap_schema(schemaName(schema), kEncoding, ss.str());
  writer.addSchema(mcap_schema);
  mcap::Channel channel(schema.channel_name, kEncoding, mcap_schema.id);
  writer.addChannel(channel);
  return channel.id;
}

/// Size of a message body holding `mask_size` mask bytes and `payload_size`
/// payload bytes.
inline size_t messageBodySize(size_t mask_size, size_t payload_size)
{
  return 2 * sizeof(uint32_t) + mask_size + payload_size;
}

/// Write the message body `u32 mask_len, mask, u32 payload_len, payload`
/// (lengths little-endian) into `body`, resized to fit. Does not allocate when
/// `body` already has the capacity. Throws std::length_error if a length does
/// not fit in 32 bits.
inline void encodeMessageBody(ByteSpan mask, ByteSpan payload, std::vector<uint8_t>& body)
{
  constexpr size_t kMaxLength = std::numeric_limits<uint32_t>::max();
  if(mask.size > kMaxLength || payload.size > kMaxLength)
  {
    throw std::length_error("data_tamer MCAP message: mask or payload too large");
  }
  body.resize(messageBodySize(mask.size, payload.size));
  uint8_t* out = body.data();
  const auto put = [&out](ByteSpan bytes) {
    const auto length = static_cast<uint32_t>(bytes.size);
    for(size_t i = 0; i < sizeof(uint32_t); ++i)
    {
      *out++ = static_cast<uint8_t>(length >> (8 * i));
    }
    if(bytes.size > 0)
    {
      std::copy(bytes.data, bytes.data + bytes.size, out);
      out += bytes.size;
    }
  };
  put(mask);
  put(payload);
}

/// Write one message from its parts: `timestamp` (nanoseconds since the epoch)
/// becomes logTime and publishTime. `scratch` holds the encoded body; reuse it
/// across calls to avoid allocations. Returns the writer's status.
inline mcap::Status writeMessage(mcap::McapWriter& writer, mcap::ChannelId channel_id,
                                 uint32_t sequence, std::chrono::nanoseconds timestamp,
                                 ByteSpan mask, ByteSpan payload,
                                 std::vector<uint8_t>& scratch)
{
  encodeMessageBody(mask, payload, scratch);
  mcap::Message msg;
  msg.channelId = channel_id;
  msg.sequence = sequence;
  msg.logTime = mcap::Timestamp(timestamp.count());
  msg.publishTime = msg.logTime;
  msg.data = reinterpret_cast<const std::byte*>(scratch.data());  // NOLINT
  msg.dataSize = scratch.size();
  return writer.write(msg);
}

/// writeMessage() for a Snapshot, as received by DataSink::onSnapshot()
/// (`*ref`) or stored by the application.
inline mcap::Status writeSnapshot(mcap::McapWriter& writer, mcap::ChannelId channel_id,
                                  uint32_t sequence, const Snapshot& snapshot,
                                  std::vector<uint8_t>& scratch)
{
  return writeMessage(writer, channel_id, sequence, snapshot.timestamp,
                      snapshot.active_mask, snapshot.payload, scratch);
}

}  // namespace DataTamer::mcap_encoding
