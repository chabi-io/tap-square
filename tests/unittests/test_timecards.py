import unittest
from copy import deepcopy
from unittest.mock import patch, MagicMock
from singer import Transformer

from tap_square.client import SquareClient, should_giveup_api_error
from tap_square.streams import Timecards


mock_config = {
    "sandbox": "true",
    "start_date": "2026-01-01T00:00:00Z",
    "refresh_token": "123456789",
    "client_id": "abcdefgh",
    "client_secret": "abc1234",
}

stream_schema = {
    "type": ["null", "object"],
    "properties": {
        "id": {"type": ["null", "string"]},
        "team_member_id": {"type": ["null", "string"]},
        "location_id": {"type": ["null", "string"]},
        "status": {"type": ["null", "string"]},
        "created_at": {"format": "date-time", "type": ["null", "string"]},
        "updated_at": {"format": "date-time", "type": ["null", "string"]},
    },
}

stream_metadata = {
    (): {
        "table-key-properties": ["id"],
        "forced-replication-method": "INCREMENTAL",
        "selected": True,
        "inclusion": "available",
        "valid-replication-keys": ["updated_at"],
    },
    ("properties", "id"): {"inclusion": "automatic"},
    ("properties", "team_member_id"): {"inclusion": "available"},
    ("properties", "location_id"): {"inclusion": "available"},
    ("properties", "status"): {"inclusion": "available"},
    ("properties", "created_at"): {"inclusion": "available"},
    ("properties", "updated_at"): {"inclusion": "automatic"},
}


def _timecard(tc_id, updated_at):
    return {
        "id": tc_id,
        "team_member_id": "tm_1",
        "location_id": "loc_1",
        "status": "CLOSED",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": updated_at,
    }


class TestTimecardsSync(unittest.TestCase):
    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    def test_sync_writes_bookmark_of_max_updated_at(self, _mocked_access_token):
        """The returned state should bookmark the maximum `updated_at` seen."""
        pages = [
            (
                [
                    _timecard("a", "2026-02-01T00:00:00Z"),
                    _timecard("b", "2026-03-01T00:00:00Z"),
                ],
                None,
            )
        ]

        client = SquareClient(mock_config, "config_path")
        with patch.object(client, "get_timecards", return_value=iter(pages)):
            timecards_obj = Timecards(client)
            with Transformer() as transformer:
                return_value = timecards_obj.sync(
                    {"currently_syncing": "timecards"},
                    stream_schema,
                    stream_metadata,
                    mock_config,
                    transformer,
                )

        # The bookmark is taken from the transformed record, which the singer
        # Transformer normalizes to microsecond precision.
        self.assertEqual(
            {
                "currently_syncing": "timecards",
                "bookmarks": {"timecards": {"updated_at": "2026-03-01T00:00:00.000000Z"}},
            },
            return_value,
        )

    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    @patch("tap_square.streams.singer.write_record")
    def test_sync_filters_records_before_start_time(self, mocked_write_record, _mocked_access_token):
        """Records older than the bookmark/start_time should not be written."""
        pages = [
            (
                [
                    _timecard("old", "2025-12-01T00:00:00Z"),  # before start_date
                    _timecard("new", "2026-02-01T00:00:00Z"),  # after start_date
                ],
                None,
            )
        ]

        client = SquareClient(mock_config, "config_path")
        with patch.object(client, "get_timecards", return_value=iter(pages)):
            timecards_obj = Timecards(client)
            with Transformer() as transformer:
                return_value = timecards_obj.sync(
                    {},
                    stream_schema,
                    stream_metadata,
                    mock_config,
                    transformer,
                )

        written_ids = [call.args[1]["id"] for call in mocked_write_record.call_args_list]
        self.assertEqual(["new"], written_ids)
        self.assertEqual(
            "2026-02-01T00:00:00.000000Z",
            return_value["bookmarks"]["timecards"]["updated_at"],
        )


class TestTimecardsDescendingSync(unittest.TestCase):
    """Syncs resuming from a bookmark page DESC and stop at the first older record."""

    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    def test_first_run_requests_ascending_and_resume_requests_descending(self, _mocked_access_token):
        client = SquareClient(mock_config, "config_path")
        timecards_obj = Timecards(client)

        for state, expected_order in [
                ({}, "ASC"),
                ({"bookmarks": {"timecards": {"updated_at": "2026-02-01T00:00:00.000000Z"}}}, "DESC"),
        ]:
            with patch.object(client, "get_timecards", return_value=iter([])) as mocked_get:
                with Transformer() as transformer:
                    timecards_obj.sync(state, stream_schema, stream_metadata, mock_config, transformer)
            self.assertEqual((expected_order,), mocked_get.call_args.args)

    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    def test_first_run_uses_ascending_even_when_bookmark_equals_start_date(self, _mocked_access_token):
        """A stored bookmark drives the ordering, not its value relative to start_date.

        A location with no timecards keeps re-writing a bookmark equal to `start_date`;
        that is still a resumed sync and should page DESC.
        """
        client = SquareClient(mock_config, "config_path")
        state = {"bookmarks": {"timecards": {"updated_at": mock_config["start_date"]}}}

        with patch.object(client, "get_timecards", return_value=iter([])) as mocked_get:
            timecards_obj = Timecards(client)
            with Transformer() as transformer:
                timecards_obj.sync(state, stream_schema, stream_metadata, mock_config, transformer)

        self.assertEqual(("DESC",), mocked_get.call_args.args)

    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    @patch("tap_square.streams.singer.write_record")
    def test_descending_sync_stops_at_first_record_older_than_bookmark(
            self, mocked_write_record, _mocked_access_token):
        """Iteration stops at the first stale record; later pages are never requested."""
        pages = iter([
            ([_timecard("new_1", "2026-05-01T00:00:00Z"),
              _timecard("new_2", "2026-04-01T00:00:00Z")], "CURSOR_1"),
            ([_timecard("new_3", "2026-03-15T00:00:00Z"),
              _timecard("stale_1", "2026-01-15T00:00:00Z"),
              _timecard("stale_2", "2026-01-10T00:00:00Z")], "CURSOR_2"),
            ([_timecard("never_read", "2026-01-05T00:00:00Z")], None),
        ])
        state = {"bookmarks": {"timecards": {"updated_at": "2026-03-01T00:00:00.000000Z"}}}

        client = SquareClient(mock_config, "config_path")
        with patch.object(client, "get_timecards", return_value=pages):
            timecards_obj = Timecards(client)
            with Transformer() as transformer:
                return_value = timecards_obj.sync(
                    state, stream_schema, stream_metadata, mock_config, transformer)

        written_ids = [call.args[1]["id"] for call in mocked_write_record.call_args_list]
        self.assertEqual(["new_1", "new_2", "new_3"], written_ids)
        # The generator was abandoned before its third page, so that page was never fetched.
        self.assertEqual([("never_read", None)], [(p[0][0]["id"], p[1]) for p in pages])
        self.assertEqual(
            "2026-05-01T00:00:00.000000Z",
            return_value["bookmarks"]["timecards"]["updated_at"],
        )

    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    @patch("tap_square.streams.singer.write_state")
    def test_descending_sync_writes_state_only_after_the_last_page(
            self, mocked_write_state, _mocked_access_token):
        """The DESC bookmark must not be committed while older pages are still pending.

        The first page holds the newest records, so a per-page write would commit a
        watermark covering records that later pages have not emitted yet -- a failure
        partway through would then skip them permanently.
        """
        pages = [
            ([_timecard("a", "2026-05-01T00:00:00Z")], "CURSOR_1"),
            ([_timecard("b", "2026-04-01T00:00:00Z")], "CURSOR_2"),
            ([_timecard("c", "2026-03-15T00:00:00Z")], None),
        ]
        state = {"bookmarks": {"timecards": {"updated_at": "2026-03-01T00:00:00.000000Z"}}}

        client = SquareClient(mock_config, "config_path")
        with patch.object(client, "get_timecards", return_value=iter(pages)):
            timecards_obj = Timecards(client)
            with Transformer() as transformer:
                timecards_obj.sync(state, stream_schema, stream_metadata, mock_config, transformer)

        self.assertEqual(1, mocked_write_state.call_count)
        self.assertEqual(
            "2026-05-01T00:00:00.000000Z",
            mocked_write_state.call_args.args[0]["bookmarks"]["timecards"]["updated_at"],
        )

    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    @patch("tap_square.streams.singer.write_state")
    def test_descending_sync_leaves_bookmark_untouched_when_it_fails_midway(
            self, mocked_write_state, _mocked_access_token):
        """A DESC run that dies partway commits nothing, so the next run redoes it."""
        def failing_pages():
            yield ([_timecard("a", "2026-05-01T00:00:00Z")], "CURSOR_1")
            raise RuntimeError("Square API blew up on page 2")

        state = {"bookmarks": {"timecards": {"updated_at": "2026-03-01T00:00:00.000000Z"}}}

        client = SquareClient(mock_config, "config_path")
        with patch.object(client, "get_timecards", return_value=failing_pages()):
            timecards_obj = Timecards(client)
            with Transformer() as transformer:
                with self.assertRaises(RuntimeError):
                    timecards_obj.sync(
                        state, stream_schema, stream_metadata, mock_config, transformer)

        mocked_write_state.assert_not_called()
        self.assertEqual(
            "2026-03-01T00:00:00.000000Z",
            state["bookmarks"]["timecards"]["updated_at"],
        )

    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    @patch("tap_square.streams.singer.write_state")
    def test_ascending_sync_still_writes_state_after_every_page(
            self, mocked_write_state, _mocked_access_token):
        """ASC ordering keeps per-page bookmarks -- the running max is a real watermark."""
        # `write_bookmark` mutates and returns the same state dict, so `call_args_list`
        # would hold N references to one object that has reached its final value by the
        # time it is read. Snapshot each call instead.
        emitted = []
        mocked_write_state.side_effect = lambda st: emitted.append(deepcopy(st))

        pages = [
            ([_timecard("a", "2026-02-01T00:00:00Z")], "CURSOR_1"),
            ([_timecard("b", "2026-03-01T00:00:00Z")], None),
        ]

        client = SquareClient(mock_config, "config_path")
        with patch.object(client, "get_timecards", return_value=iter(pages)):
            timecards_obj = Timecards(client)
            with Transformer() as transformer:
                timecards_obj.sync({}, stream_schema, stream_metadata, mock_config, transformer)

        self.assertEqual(
            ["2026-02-01T00:00:00.000000Z", "2026-03-01T00:00:00.000000Z"],
            [st["bookmarks"]["timecards"]["updated_at"] for st in emitted],
        )

    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    @patch("tap_square.streams.singer.write_record")
    def test_ascending_sync_does_not_stop_at_a_record_older_than_start_time(
            self, mocked_write_record, _mocked_access_token):
        """The early exit is DESC-only: under ASC a stale record is skipped, not terminal."""
        pages = [
            ([_timecard("old", "2025-12-01T00:00:00Z"),
              _timecard("new", "2026-02-01T00:00:00Z")], None),
        ]

        client = SquareClient(mock_config, "config_path")
        with patch.object(client, "get_timecards", return_value=iter(pages)):
            timecards_obj = Timecards(client)
            with Transformer() as transformer:
                timecards_obj.sync({}, stream_schema, stream_metadata, mock_config, transformer)

        self.assertEqual(["new"], [call.args[1]["id"] for call in mocked_write_record.call_args_list])

    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    @patch("tap_square.streams.singer.write_record")
    def test_record_exactly_at_the_bookmark_is_re_emitted_not_treated_as_stale(
            self, mocked_write_record, _mocked_access_token):
        """The bound is inclusive, so the boundary record duplicates rather than ending the sync."""
        pages = [
            ([_timecard("boundary", "2026-03-01T00:00:00Z"),
              _timecard("stale", "2026-02-01T00:00:00Z")], "CURSOR_1"),
        ]
        state = {"bookmarks": {"timecards": {"updated_at": "2026-03-01T00:00:00.000000Z"}}}

        client = SquareClient(mock_config, "config_path")
        with patch.object(client, "get_timecards", return_value=iter(pages)):
            timecards_obj = Timecards(client)
            with Transformer() as transformer:
                timecards_obj.sync(state, stream_schema, stream_metadata, mock_config, transformer)

        self.assertEqual(
            ["boundary"], [call.args[1]["id"] for call in mocked_write_record.call_args_list])


class TestGetTimecardsClient(unittest.TestCase):
    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    def test_get_timecards_paginates_and_converts_models_to_dicts(self, _mocked_access_token):
        """get_timecards should follow the cursor and convert SDK models via .dict()."""
        def make_model(tc_id):
            model = MagicMock()
            model.dict.return_value = _timecard(tc_id, "2026-02-01T00:00:00Z")
            return model

        first_response = MagicMock(timecards=[make_model("a")], cursor="CURSOR_1")
        second_response = MagicMock(timecards=[make_model("b")], cursor=None)

        client = SquareClient(mock_config, "config_path")
        client._new_client = MagicMock()
        client._new_client.labor.search_timecards.side_effect = [first_response, second_response]

        pages = list(client.get_timecards())

        # Two pages yielded, then iteration stops because the second cursor is None
        self.assertEqual(2, len(pages))
        self.assertEqual([{"id": "a", "cursor": "CURSOR_1"}],
                         [{"id": pages[0][0][0]["id"], "cursor": pages[0][1]}])
        self.assertEqual("b", pages[1][0][0]["id"])
        self.assertIsNone(pages[1][1])

        # First call uses cursor=None, second uses the cursor returned by the first page
        first_call_kwargs = client._new_client.labor.search_timecards.call_args_list[0].kwargs
        second_call_kwargs = client._new_client.labor.search_timecards.call_args_list[1].kwargs
        self.assertIsNone(first_call_kwargs["cursor"])
        self.assertEqual("CURSOR_1", second_call_kwargs["cursor"])
        self.assertEqual(200, first_call_kwargs["limit"])


class TestShouldGiveupApiError(unittest.TestCase):
    def test_retries_only_rate_limit_and_server_errors(self):
        retryable = [MagicMock(status_code=429), MagicMock(status_code=500), MagicMock(status_code=503)]
        non_retryable = [MagicMock(status_code=400), MagicMock(status_code=401),
                         MagicMock(status_code=403), MagicMock(status_code=410)]

        for ex in retryable:
            self.assertFalse(should_giveup_api_error(ex))
        for ex in non_retryable:
            self.assertTrue(should_giveup_api_error(ex))
