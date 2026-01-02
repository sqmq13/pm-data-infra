from pm_arb.book import OrderBook
from pm_arb.reconcile import Reconciler


def _make_book(token_id: str, price_micro: int, size_units: int) -> OrderBook:
    book = OrderBook(token_id=token_id)
    book.update_from_asks([(price_micro, size_units)], top_k=50)
    return book


def test_reconcile_persist_desync():
    recon = Reconciler(persist_n=3, tick_tolerance=0, rel_tol=0.0)
    ws_book = _make_book("tokenA", 500_000, 10 * 1_000_000)
    rest_book = _make_book("tokenA", 800_000, 10 * 1_000_000)
    sizes = [10]
    result = recon.compare("tokenA", ws_book, rest_book, sizes)
    assert result.mismatch
    assert not result.desynced
    result = recon.compare("tokenA", ws_book, rest_book, sizes)
    assert not result.desynced
    result = recon.compare("tokenA", ws_book, rest_book, sizes)
    assert result.desynced


def test_reconcile_immediate_desync():
    recon = Reconciler(persist_n=3, tick_tolerance=0, rel_tol=0.0)
    ws_book = _make_book("tokenA", 400_000, 10 * 1_000_000)
    rest_book = _make_book("tokenA", 1_500_000, 10 * 1_000_000)
    result = recon.compare("tokenA", ws_book, rest_book, [10])
    assert result.desynced
