"""Tests for Bluetooth source routing."""

import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "ha_tion_btle"
    / "source_routing.py"
)
SPEC = importlib.util.spec_from_file_location("tion_source_routing", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
SOURCE_ROUTING = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SOURCE_ROUTING)
order_connection_candidates = SOURCE_ROUTING.order_connection_candidates


class SourceRoutingTest(unittest.TestCase):
    """Verify source pinning and backward-compatible automatic routing."""

    def test_strict_source_excludes_unpaired_fallbacks(self) -> None:
        candidates = [
            ("realtek", "8C:68:8B:81:88:77", -94),
            ("atom", "E8:F6:0A:98:F8:A6", -87),
            ("intel", "94:E2:3C:66:14:A7", -80),
        ]

        self.assertEqual(
            order_connection_candidates(
                candidates,
                preferred_source="94:E2:3C:66:14:A7",
                configured_source="E8:F6:0A:98:F8:A6",
                strict_source=True,
            ),
            [("atom", "E8:F6:0A:98:F8:A6", -87)],
        )

    def test_strict_source_returns_empty_when_it_is_not_visible(self) -> None:
        candidates = [("intel", "94:E2:3C:66:14:A7", -80)]

        self.assertEqual(
            order_connection_candidates(
                candidates,
                preferred_source="94:E2:3C:66:14:A7",
                configured_source="E8:F6:0A:98:F8:A6",
                strict_source=True,
            ),
            [],
        )

    def test_preferred_source_allows_fallback_when_not_strict(self) -> None:
        candidates = [
            ("realtek", "8C:68:8B:81:88:77", -70),
            ("atom", "E8:F6:0A:98:F8:A6", -90),
        ]

        self.assertEqual(
            order_connection_candidates(
                candidates,
                preferred_source="8C:68:8B:81:88:77",
                configured_source="E8:F6:0A:98:F8:A6",
                strict_source=False,
            )[0][0],
            "atom",
        )

    def test_automatic_mode_preserves_home_assistant_preference(self) -> None:
        candidates = [
            ("realtek", "8C:68:8B:81:88:77", -70),
            ("atom", "E8:F6:0A:98:F8:A6", -90),
        ]

        self.assertEqual(
            order_connection_candidates(
                candidates,
                preferred_source="E8:F6:0A:98:F8:A6",
                configured_source="auto",
                strict_source=False,
            )[0][0],
            "atom",
        )


if __name__ == "__main__":
    unittest.main()
