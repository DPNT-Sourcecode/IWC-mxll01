from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])

def test_legacy_rule_of_3() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(base="2025-10-20 12:00:00")).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(base="2025-10-20 12:00:00")).expect(1),
        call_enqueue("id_verification", 2, iso_ts(base="2025-10-20 12:00:00")).expect(1),
        call_size().expect(1),
        call_dequeue().expect("legacy_rule_of_3", 42),
    ])


