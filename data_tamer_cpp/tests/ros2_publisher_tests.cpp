#include "data_tamer/data_tamer.hpp"
#include "data_tamer/sinks/ros2_publisher_sink.hpp"

#include "data_tamer_msgs/msg/snapshot_batch.hpp"
#include "data_tamer_parser/data_tamer_parser.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>

using namespace DataTamer;

TEST(DataTamerROS2Publisher, SharedPointer)
{
  auto node = std::make_shared<rclcpp::Node>("test_datatamer_shared_pointer");
  auto ros2_sink = ROS2PublisherSink::create(node, "test_shared_pointer");

  auto channel = ChannelsRegistry::Global().getChannel("channel_shared_pointer");

  channel->addDataSink(ros2_sink);

  double const value = 1.;
  channel->registerValue("value", &value);

  EXPECT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
}

TEST(DataTamerROS2Publisher, SharedPointerLifeCycle)
{
  auto lifecycle_node = std::make_shared<rclcpp_lifecycle::LifecycleNode>("test_"
                                                                          "datatamer_"
                                                                          "shared_"
                                                                          "pointer_"
                                                                          "lifecycle");
  auto ros2_sink = ROS2PublisherSink::create(lifecycle_node, "test_shared_"
                                                             "pointer_"
                                                             "lifecycle");

  auto channel = ChannelsRegistry::Global().getChannel("channel_shared_pointer_"
                                                       "lifecycle");

  channel->addDataSink(ros2_sink);

  double const value = 1.;
  channel->registerValue("value", &value);

  EXPECT_EQ(channel->takeSnapshot(), SnapshotResult::ok);

  lifecycle_node->shutdown();
}

TEST(DataTamerROS2Publisher, Dereference)
{
  auto node = std::make_shared<rclcpp::Node>("test_datatamer_dereference");
  auto ros2_sink = ROS2PublisherSink::create(*node, "test_dereference");

  auto channel = ChannelsRegistry::Global().getChannel("channel_dereference");

  channel->addDataSink(ros2_sink);

  double const value = 1.;
  channel->registerValue("value", &value);

  EXPECT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
}

TEST(DataTamerROS2Publisher, DereferenceLifeCycle)
{
  auto lifecycle_node = std::make_shared<rclcpp_lifecycle::LifecycleNode>("test_"
                                                                          "datatamer_"
                                                                          "dereference_"
                                                                          "lifecycle");
  auto ros2_sink = ROS2PublisherSink::create(*lifecycle_node, "test_"
                                                              "dereference_"
                                                              "lifecycle");

  auto channel = ChannelsRegistry::Global().getChannel("channel_dereference_lifecycle");

  channel->addDataSink(ros2_sink);

  double const value = 1.;
  channel->registerValue("value", &value);

  EXPECT_EQ(channel->takeSnapshot(), SnapshotResult::ok);

  lifecycle_node->shutdown();
}

TEST(DataTamerROS2Publisher, NodeInterfacesDirect)
{
  auto node = std::make_shared<rclcpp::Node>("test_datatamer_node_interfaces");
  PublisherNodeInterfaces interfaces(*node);
  auto ros2_sink = ROS2PublisherSink::create(interfaces, "test_node_"
                                                         "interfaces");

  auto channel = ChannelsRegistry::Global().getChannel("channel_node_interfaces");

  channel->addDataSink(ros2_sink);

  double const value = 1.;
  channel->registerValue("value", &value);

  EXPECT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
}

TEST(DataTamerROS2Publisher, NodeInterfacesDirectLifeCycle)
{
  auto lifecycle_node = std::make_shared<rclcpp_lifecycle::LifecycleNode>("test_"
                                                                          "datatamer_"
                                                                          "node_"
                                                                          "interfaces_"
                                                                          "lifecycle");
  PublisherNodeInterfaces interfaces(*lifecycle_node);
  auto ros2_sink = ROS2PublisherSink::create(interfaces, "test_node_interfaces_"
                                                         "lifecycle");

  auto channel = ChannelsRegistry::Global().getChannel("channel_node_interfaces_"
                                                       "lifecycle");

  channel->addDataSink(ros2_sink);

  double const value = 1.;
  channel->registerValue("value", &value);

  EXPECT_EQ(channel->takeSnapshot(), SnapshotResult::ok);

  lifecycle_node->shutdown();
}

namespace
{
using BatchMsg = data_tamer_msgs::msg::SnapshotBatch;

// Calls `produce` and spins until a batch arrives on `<prefix>/data_batch`.
// Retries because a publisher drops messages sent before it matched the subscriber.
template <typename Produce>
std::optional<BatchMsg> receiveBatch(const std::shared_ptr<rclcpp::Node>& node,
                                     const std::string& prefix, Produce&& produce)
{
  std::optional<BatchMsg> received;
  auto sub = node->create_subscription<BatchMsg>(
      prefix + "/data_batch", rclcpp::QoS(rclcpp::KeepAll()), [&](const BatchMsg& msg) {
        if(!received)
        {
          received = msg;
        }
      });
  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(node);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while(!received && std::chrono::steady_clock::now() < deadline)
  {
    produce();
    executor.spin_some(std::chrono::milliseconds(100));
  }
  return received;
}
}  // namespace

TEST(DataTamerROS2Publisher, AggregateBySize)
{
  auto node = std::make_shared<rclcpp::Node>("test_datatamer_aggregate_size");
  ROS2PublisherOptions options;
  options.aggregate = true;
  options.max_batch_size = 5;
  options.max_batch_delay = std::chrono::milliseconds(0);
  auto ros2_sink = ROS2PublisherSink::create(node, "test_aggregate_size", options);

  auto channel = ChannelsRegistry::Global().getChannel("channel_aggregate_size");
  channel->addDataSink(ros2_sink);
  double value = 1.;
  channel->registerValue("value", &value);

  auto batch = receiveBatch(node, "test_aggregate_size", [&] {
    for(int i = 0; i < 5; i++)
    {
      value += 1.0;
      ASSERT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
    }
    ros2_sink->drain();
  });

  ASSERT_TRUE(batch.has_value());
  ASSERT_EQ(batch->snapshots.size(), 5u);
  ASSERT_EQ(batch->schemas.size(), 1u);
  EXPECT_EQ(batch->schemas[0].channel_name, "channel_aggregate_size");
  EXPECT_EQ(batch->schemas[0].hash, channel->getSchema().hash);
  EXPECT_FALSE(batch->schemas[0].schema_text.empty());
  for(const auto& snapshot : batch->snapshots)
  {
    EXPECT_EQ(snapshot.schema_hash, channel->getSchema().hash);
    EXPECT_EQ(snapshot.payload.size(), sizeof(double));
  }

  // decode with the parser helpers: the batch is self-contained
  DataTamerParser::SchemaRegistry registry;
  std::vector<double> values;
  const size_t visited = DataTamerParser::ForEachSnapshotInBatch(
      registry, *batch,
      [&](const DataTamerParser::Schema& schema,
          const DataTamerParser::SnapshotView& view) {
        EXPECT_TRUE(DataTamerParser::ParseSnapshot(
            schema, view,
            [&](const std::string& name, const DataTamerParser::VarNumber& n) {
              EXPECT_EQ(name, "value");
              values.push_back(std::get<double>(n));
            }));
      });
  EXPECT_EQ(visited, 5u);
  ASSERT_EQ(values.size(), 5u);
  for(size_t i = 1; i < values.size(); i++)
  {
    EXPECT_EQ(values[i], values[i - 1] + 1.0);
  }
}

TEST(DataTamerROS2Publisher, AggregateFlushWithoutSchemas)
{
  auto node = std::make_shared<rclcpp::Node>("test_datatamer_aggregate_flush");
  ROS2PublisherOptions options;
  options.aggregate = true;
  options.max_batch_size = 1000;
  options.max_batch_delay = std::chrono::milliseconds(0);
  options.embed_schemas = false;
  auto ros2_sink = ROS2PublisherSink::create(node, "test_aggregate_flush", options);

  auto channel = ChannelsRegistry::Global().getChannel("channel_aggregate_flush");
  channel->addDataSink(ros2_sink);
  double const value = 1.;
  channel->registerValue("value", &value);

  auto batch = receiveBatch(node, "test_aggregate_flush", [&] {
    for(int i = 0; i < 3; i++)
    {
      ASSERT_EQ(channel->takeSnapshot(), SnapshotResult::ok);
    }
    ros2_sink->drain();
    ros2_sink->as<ROS2PublisherSink>().flush();
  });

  ASSERT_TRUE(batch.has_value());
  EXPECT_EQ(batch->snapshots.size(), 3u);
  EXPECT_TRUE(batch->schemas.empty());
}

int main(int argc, char** argv)
{
  rclcpp::init(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  rclcpp::shutdown();
  return ret;
}
