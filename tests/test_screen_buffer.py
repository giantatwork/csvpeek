from csvpeek.screen_buffer import ScreenBuffer


class FetchRecorder:
    def __init__(self, total_rows: int) -> None:
        self.total_rows = total_rows
        self.calls: list[tuple[int, int]] = []

    def __call__(self, start_row: int, fetch_size: int) -> list[tuple[int]]:
        self.calls.append((start_row, fetch_size))
        end = min(start_row + fetch_size, self.total_rows)
        return [(i,) for i in range(start_row, end)]


def test_fetch_and_clamp_high_offset() -> None:
    fetcher = FetchRecorder(total_rows=10)
    buffer = ScreenBuffer(fetcher)

    rows, offset = buffer.get_page_rows(
        desired_start=8,
        page_size=4,
        total_rows=10,
        fetch_size=6,
    )

    assert offset == 6  # clamped to last full page
    assert fetcher.calls == [(6, 6)]
    assert rows == [(6,), (7,), (8,), (9,)]


def test_reuses_buffer_within_window() -> None:
    fetcher = FetchRecorder(total_rows=10)
    buffer = ScreenBuffer(fetcher)

    first_rows, first_offset = buffer.get_page_rows(
        desired_start=0,
        page_size=3,
        total_rows=10,
        fetch_size=5,
    )

    second_rows, second_offset = buffer.get_page_rows(
        desired_start=2,
        page_size=3,
        total_rows=10,
        fetch_size=5,
    )

    assert first_rows == [(0,), (1,), (2,)]
    assert second_rows == [(2,), (3,), (4,)]
    assert first_offset == 0
    assert second_offset == 2
    assert fetcher.calls == [(0, 5)]  # second call hit cache


def test_reset_forces_refetch() -> None:
    fetcher = FetchRecorder(total_rows=10)
    buffer = ScreenBuffer(fetcher)

    buffer.get_page_rows(
        desired_start=0,
        page_size=3,
        total_rows=10,
        fetch_size=5,
    )
    buffer.reset()
    buffer.get_page_rows(
        desired_start=0,
        page_size=3,
        total_rows=10,
        fetch_size=5,
    )

    assert fetcher.calls == [(0, 5), (0, 5)]


def test_fetches_when_request_exits_cached_window() -> None:
    fetcher = FetchRecorder(total_rows=10)
    buffer = ScreenBuffer(fetcher)

    buffer.get_page_rows(
        desired_start=0,
        page_size=3,
        total_rows=10,
        fetch_size=5,
    )
    rows, offset = buffer.get_page_rows(
        desired_start=5,
        page_size=3,
        total_rows=10,
        fetch_size=5,
    )

    assert offset == 5
    assert rows == [(5,), (6,), (7,)]
    assert fetcher.calls == [(0, 5), (5, 5)]


def test_small_total_rows_smaller_than_page() -> None:
    fetcher = FetchRecorder(total_rows=2)
    buffer = ScreenBuffer(fetcher)

    rows, offset = buffer.get_page_rows(
        desired_start=0,
        page_size=5,
        total_rows=2,
        fetch_size=5,
    )

    assert offset == 0
    assert rows == [(0,), (1,)]
    assert fetcher.calls == [(0, 5)]
