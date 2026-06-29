import unittest
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
