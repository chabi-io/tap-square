import unittest
from unittest.mock import patch, MagicMock
from singer import Transformer

from tap_square.client import SquareClient
from tap_square.streams import Refunds


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
        "status": {"type": ["null", "string"]},
        "location_id": {"type": ["null", "string"]},
        "payment_id": {"type": ["null", "string"]},
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
    ("properties", "status"): {"inclusion": "available"},
    ("properties", "location_id"): {"inclusion": "available"},
    ("properties", "payment_id"): {"inclusion": "available"},
    ("properties", "created_at"): {"inclusion": "available"},
    ("properties", "updated_at"): {"inclusion": "automatic"},
}


def _refund(refund_id, updated_at):
    return {
        "id": refund_id,
        "status": "COMPLETED",
        "location_id": "loc_1",
        "payment_id": "pay_1",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": updated_at,
    }


class TestRefundsSync(unittest.TestCase):
    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    def test_sync_bookmarks_max_updated_at(self, _mocked_access_token):
        pages = [
            ([_refund("a", "2026-02-01T00:00:00Z"), _refund("b", "2026-03-01T00:00:00Z")], None),
        ]

        client = SquareClient(mock_config, "config_path")
        with patch.object(client, "get_refunds", return_value=iter(pages)):
            refunds_obj = Refunds(client)
            with Transformer() as transformer:
                return_value = refunds_obj.sync(
                    {"currently_syncing": "refunds"},
                    stream_schema,
                    stream_metadata,
                    mock_config,
                    transformer,
                )

        self.assertEqual(
            {
                "currently_syncing": "refunds",
                "bookmarks": {"refunds": {"updated_at": "2026-03-01T00:00:00.000000Z"}},
            },
            return_value,
        )


class TestGetRefundsClient(unittest.TestCase):
    @patch("tap_square.client.SquareClient._get_access_token", return_value="mock_token")
    def test_get_refunds_requests_updated_at_filter_and_sort(self, _mocked_access_token):
        """get_refunds should filter/sort on updated_at (not created_at begin_time)."""
        result = MagicMock()
        result.is_error.return_value = False
        result.body = {"refunds": [_refund("a", "2026-02-01T00:00:00Z")], "cursor": None}

        client = SquareClient(mock_config, "config_path")
        client._client = MagicMock()
        client._client.refunds.list_payment_refunds.return_value = result

        pages = list(client.get_refunds("2026-01-15T00:00:00Z"))

        self.assertEqual(1, len(pages))
        kwargs = client._client.refunds.list_payment_refunds.call_args.kwargs
        self.assertEqual("UPDATED_AT", kwargs["sort_field"])
        self.assertEqual("ASC", kwargs["sort_order"])
        # Filters by updated_at, not the created_at-based begin_time
        self.assertIn("updated_at_begin_time", kwargs)
        self.assertNotIn("begin_time", kwargs)
        # The begin time is the bookmark moved back by 1ms (exclusive param safety)
        self.assertEqual("2026-01-14T23:59:59.999000Z", kwargs["updated_at_begin_time"])
