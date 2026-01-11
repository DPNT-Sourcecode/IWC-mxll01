from __future__ import annotations

from .utils import call_age, call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])

def test_legacy_rule_of_3() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts()).expect(1),
        call_enqueue("bank_statements", 2, iso_ts()).expect(2),
        call_enqueue("id_verification", 1, iso_ts()).expect(3),
        call_enqueue("bank_statements", 1, iso_ts()).expect(4),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])

def test_legacy_timestamp_ordering() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts()).expect(2),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])

def test_legacy_dependency_resolution() -> None:
    run_queue([
        call_enqueue("credit_check", 1, iso_ts()).expect(2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
    ])

def test_legacy_deduplication() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts()).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 1),
    ])

def test_legacy_deprioritize_bank_statements() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts()).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=2)).expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("bank_statements", 1),
    ])

def test_legacy_age() -> None:
    run_queue([
        call_enqueue("id_verification", 1, iso_ts()).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        call_age().expect(300),
    ])

def test_legacy_time_sensitive_bank_statements() -> None:
    run_queue([
        call_enqueue("id_verification", 1, iso_ts()).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("companies_house", 3, iso_ts(delta_minutes=7)).expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 3),
    ])

def test_legacy_time_sensitive_bank_statements_r5_s6() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts()).expect(1),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=6)).expect(3),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=7)).expect(4),
        # call_dequeue().expect("bank_statements", 1),
        # call_dequeue().expect("companies_house", 2),
        # call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("bank_statements", 2),
    ])
