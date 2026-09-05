"""Pure helpers for selecting a Home Assistant Bluetooth scanner source."""

from __future__ import annotations

from typing import TypeVar

CandidateT = TypeVar("CandidateT")
BLUETOOTH_SOURCE_AUTO = "auto"


def order_connection_candidates(
    candidates: list[tuple[CandidateT, str | None, int]],
    preferred_source: str | None,
    configured_source: str,
    strict_source: bool,
) -> list[tuple[CandidateT, str | None, int]]:
    """Order candidates, optionally restricting them to one bonded source."""
    if configured_source != BLUETOOTH_SOURCE_AUTO:
        if strict_source:
            candidates = [
                candidate
                for candidate in candidates
                if candidate[1] == configured_source
            ]
        return sorted(
            candidates,
            key=lambda candidate: (
                candidate[1] == configured_source,
                candidate[2],
            ),
            reverse=True,
        )

    return sorted(
        candidates,
        key=lambda candidate: (
            candidate[1] == preferred_source,
            candidate[2],
        ),
        reverse=True,
    )
