from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])

def test_enqueue_bank_statements_dequeue_flow() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(3),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(4),
        call_size().expect(4),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house" , 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("companies_house", 2),
    ])

def test_enqueue_bank_statements_dequeue_flow_with_age() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 6, iso_ts(delta_minutes=6)).expect(3),
        call_size().expect(3),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("companies_house" , 1),
        call_dequeue().expect("id_verification", 6),
    ])

def test_enqueue_bank_statements_dequeue_flow_with_age_2() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=6)).expect(3),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=7)).expect(4),
        call_size().expect(4),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("bank_statements", 2),
    ])

def test_enqueue_bank_statements_dequeue_flow_with_age_3() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 3, iso_ts(delta_minutes=6)).expect(3),
        call_enqueue("bank_statements", 3, iso_ts(delta_minutes=7)).expect(4),
        call_size().expect(4),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("bank_statements", 2),
    ])

