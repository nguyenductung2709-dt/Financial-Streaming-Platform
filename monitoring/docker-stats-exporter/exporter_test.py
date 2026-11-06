#!/usr/bin/env python3
import unittest

import exporter

STATS_V2 = {
    "cpu_stats": {"cpu_usage": {"total_usage": 3_500_000_000}},
    "memory_stats": {"usage": 600_000_000, "stats": {"inactive_file": 100_000_000}},
    "networks": {"eth0": {"rx_bytes": 10, "tx_bytes": 20}, "eth1": {"rx_bytes": 1, "tx_bytes": 2}},
    "blkio_stats": {"io_service_bytes_recursive": [
        {"major": 8, "minor": 16, "op": "read", "value": 100},
        {"major": 8, "minor": 16, "op": "write", "value": 7},
        {"major": 8, "minor": 0, "op": "Read", "value": 50},
    ]},
}


class ParseStatsTest(unittest.TestCase):
    def test_converts_a_cgroup_v2_document(self):
        self.assertEqual(
            exporter.parse_stats(STATS_V2),
            {
                "docker_container_cpu_usage_seconds_total": 3.5,
                "docker_container_memory_working_set_bytes": 500_000_000,
                "docker_container_network_receive_bytes_total": 11,
                "docker_container_network_transmit_bytes_total": 22,
                "docker_container_block_read_bytes_total": 150,
                "docker_container_block_write_bytes_total": 7,
            },
        )

    def test_uses_total_inactive_file_on_cgroup_v1(self):
        stats = {**STATS_V2, "memory_stats": {"usage": 300, "stats": {"total_inactive_file": 100}}}
        self.assertEqual(exporter.parse_stats(stats)["docker_container_memory_working_set_bytes"], 200)

    def test_tolerates_missing_network_and_block_io(self):
        stats = {"cpu_stats": {"cpu_usage": {"total_usage": 0}}, "memory_stats": {}, "networks": None,
                 "blkio_stats": {"io_service_bytes_recursive": None}}
        values = exporter.parse_stats(stats)
        self.assertEqual(values["docker_container_network_receive_bytes_total"], 0)
        self.assertEqual(values["docker_container_block_read_bytes_total"], 0)
        self.assertEqual(values["docker_container_memory_working_set_bytes"], 0)

    def test_working_set_never_goes_negative(self):
        stats = {**STATS_V2, "memory_stats": {"usage": 10, "stats": {"inactive_file": 50}}}
        self.assertEqual(exporter.parse_stats(stats)["docker_container_memory_working_set_bytes"], 0)


class RenderTest(unittest.TestCase):
    def test_emits_help_type_and_labelled_samples(self):
        text = exporter.render([({"service": "redpanda-0", "container": "p-redpanda-0-1"}, exporter.parse_stats(STATS_V2))])
        self.assertIn("# TYPE docker_container_cpu_usage_seconds_total counter", text)
        self.assertIn("# TYPE docker_container_memory_working_set_bytes gauge", text)
        self.assertIn('docker_container_cpu_usage_seconds_total{container="p-redpanda-0-1",service="redpanda-0"} 3.5', text)
        self.assertTrue(text.endswith("\n"))

    def test_escapes_label_values(self):
        text = exporter.render([({"service": 'a"b', "container": "c"}, exporter.parse_stats(STATS_V2))])
        self.assertIn('service="a\\"b"', text)


if __name__ == "__main__":
    unittest.main()
