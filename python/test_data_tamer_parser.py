"""Decodes the golden vectors from docs/wire_format/vectors with the reference decoder."""
import array
import json
import pathlib
import unittest
from types import SimpleNamespace

import data_tamer_parser as dt

VECTORS = pathlib.Path(__file__).resolve().parent.parent / "docs" / "wire_format" / "vectors"


def read(name: str) -> bytes:
    return (VECTORS / name).read_bytes()


class GoldenVectors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = dt.parse_schema(read("schema.txt").decode(), verify_hash=True)
        cls.expected = json.loads(read("expected.json"))

    def test_schema(self):
        self.assertEqual(self.schema.channel_name, "wire_test")
        self.assertEqual(self.schema.hash, dt.schema_hash(read("schema.txt").decode()))
        self.assertEqual([f.field_name for f in self.schema.fields], self.expected["fields"])
        self.assertEqual(sorted(self.schema.custom_types), ["Point3D", "Pose"])

    def decode(self, stem: str) -> dict:
        mask, payload = read(stem + ".mask"), read(stem + ".payload")
        self.assertEqual(dt.split_mcap_message(read(stem + ".mcap_message")), (mask, payload))
        return dt.parse_snapshot(self.schema, mask, payload)

    def test_full_snapshot(self):
        self.assertEqual(self.decode("snapshot_full"), self.expected["full"])

    def test_masked_snapshot(self):
        values = self.decode("snapshot_masked")
        self.assertEqual(values, self.expected["masked"])
        self.assertNotIn("i16", values)
        self.assertNotIn("pose/stamp", values)

    def test_rejects_trailing_bytes_and_wrong_version(self):
        with self.assertRaises(ValueError):
            dt.parse_snapshot(self.schema, read("snapshot_full.mask"), read("snapshot_full.payload") + b"\0")
        with self.assertRaises(ValueError):
            dt.parse_schema("### version: 3\n")
        with self.assertRaises(ValueError):
            dt.parse_schema("### version: 5\n### hash: 1\n### channel_name: x\n", verify_hash=True)


class RosMessageHelpers(unittest.TestCase):
    """SnapshotBatch decoding, with stand-ins that have the data_tamer_msgs attributes."""

    def setUp(self):
        self.text = read("schema.txt").decode()
        self.schema_hash = dt.parse_schema(self.text).hash
        self.expected = json.loads(read("expected.json"))

    def snapshot_msg(self, stem: str, timestamp: int, schema_hash=None):
        # rclpy maps uint8[] to array('B')
        return SimpleNamespace(
            timestamp_nsec=timestamp,
            schema_hash=self.schema_hash if schema_hash is None else schema_hash,
            active_mask=array.array("B", read(stem + ".mask")),
            payload=array.array("B", read(stem + ".payload")))

    def batch(self, embed: bool):
        schemas = [SimpleNamespace(hash=self.schema_hash, channel_name="wire_test",
                                   schema_text=self.text)] if embed else []
        return SimpleNamespace(schemas=schemas, snapshots=[
            self.snapshot_msg("snapshot_full", 10),
            self.snapshot_msg("snapshot_masked", 20),
            self.snapshot_msg("snapshot_full", 30, schema_hash=self.schema_hash + 1),
        ])

    def test_self_contained_batch(self):
        registry = dt.SchemaRegistry(verify_hash=True)
        decoded = list(dt.iter_snapshot_batch(registry, self.batch(embed=True)))
        self.assertEqual(len(registry), 1)
        self.assertEqual([t for _, t, _ in decoded], [10, 20])  # unknown hash skipped
        self.assertEqual(decoded[0][0].channel_name, "wire_test")
        self.assertEqual(decoded[0][2], self.expected["full"])
        self.assertEqual(decoded[1][2], self.expected["masked"])

    def test_batch_with_schemas_from_topic(self):
        registry = dt.SchemaRegistry()
        self.assertEqual(list(dt.iter_snapshot_batch(registry, self.batch(embed=False))), [])
        registry.add_schemas(SimpleNamespace(schemas=self.batch(embed=True).schemas))
        self.assertEqual(len(list(dt.iter_snapshot_batch(registry, self.batch(embed=False)))), 2)

    def test_single_snapshot_msg(self):
        schema = dt.parse_schema(self.text)
        self.assertEqual(dt.parse_snapshot_msg(schema, self.snapshot_msg("snapshot_full", 0)),
                         self.expected["full"])


if __name__ == "__main__":
    unittest.main()
