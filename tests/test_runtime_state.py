from pm_data.runtime.events import TopOfBookUpdate
from pm_data.runtime.state import MarketState


def test_state_apply_top_of_book():
    state = MarketState(market_id="market-1")
    event = TopOfBookUpdate(
        market_id="market-1",
        bid_px_e6=1_000_000,
        bid_sz_e6=2_000_000,
        ask_px_e6=1_100_000,
        ask_sz_e6=1_500_000,
        ts_event=123,
        ts_recv=456,
        seq=1,
    )
    state.apply_top_of_book(event)
    assert state.best_bid_px_e6 == 1_000_000
    assert state.best_bid_sz_e6 == 2_000_000
    assert state.best_ask_px_e6 == 1_100_000
    assert state.best_ask_sz_e6 == 1_500_000
    assert state.last_update_seq == 1


def test_state_monotone_seq_guard():
    state = MarketState(market_id="market-1")
    first = TopOfBookUpdate(
        market_id="market-1",
        bid_px_e6=900_000,
        bid_sz_e6=1_000_000,
        ask_px_e6=1_000_000,
        ask_sz_e6=1_000_000,
        ts_event=100,
        ts_recv=200,
        seq=2,
    )
    state.apply_top_of_book(first)
    older = TopOfBookUpdate(
        market_id="market-1",
        bid_px_e6=800_000,
        bid_sz_e6=900_000,
        ask_px_e6=1_200_000,
        ask_sz_e6=900_000,
        ts_event=90,
        ts_recv=190,
        seq=1,
    )
    state.apply_top_of_book(older)
    assert state.best_bid_px_e6 == 900_000
    assert state.best_bid_sz_e6 == 1_000_000
    assert state.best_ask_px_e6 == 1_000_000
    assert state.best_ask_sz_e6 == 1_000_000
    assert state.last_update_seq == 2
