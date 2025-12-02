# tests/test_subscriber.py
import sys
import os
import unittest
from unittest.mock import patch, MagicMock
import psycopg2

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import subscriber  # Now this points to subscriber.py

class TestInsertMetrics(unittest.TestCase):

    @patch("subscriber.psycopg2.connect")
    def test_insert_metrics_from_payload(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        payload = {
            "outdoorTemp": 10.5,
            "airTemp": 21.1,
            "humidtyOutdoor": 33.0,
            "humidtyRoom": 40.1,
            "supplyAirPressure": 1.5,
            "exhaustAirPressure": 2.3,
            "supplyAirFlow": 120,
            "exhaustAirFlow": 110
        }

        subscriber.insert_metrics_from_payload(payload)

        mock_connect.assert_called_once_with(subscriber.QDB_CONN)
        mock_cursor.execute.assert_called_once()

        args, kwargs = mock_cursor.execute.call_args

        sql = args[0]
        row = args[1]

        self.assertIn("INSERT INTO Olimex_Vent", sql)
        self.assertEqual(row, (
            10.5, 21.1, 33.0, 40.1, 1.5, 2.3, 120, 110
        ))


class TestCreateTable(unittest.TestCase):

    @patch("subscriber.psycopg2.connect")
    def test_create_table_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        subscriber.create_table()

        mock_cursor.execute.assert_called_once()
        self.assertIn("CREATE TABLE IF NOT EXISTS", mock_cursor.execute.call_args[0][0])

    @patch("subscriber.time.sleep")
    @patch("subscriber.psycopg2.connect")
    def test_create_table_retries(self, mock_connect, mock_sleep):
        mock_connect.side_effect = [
            psycopg2.OperationalError("fail"),
            psycopg2.OperationalError("fail"),
            MagicMock(__enter__=lambda s: MagicMock())
        ]

        subscriber.create_table(retries=3, delay=0)

        self.assertEqual(mock_connect.call_count, 3)


class TestOnMessage(unittest.TestCase):

    @patch("subscriber.insert_metrics_from_payload")
    def test_on_message(self, mock_insert):
        msg = MagicMock()
        msg.payload.decode.return_value = '{"outdoorTemp": 5}'
        msg.topic = "spBv1.0/test"

        subscriber.on_message(None, None, msg)

        mock_insert.assert_called_once_with({"outdoorTemp": 5})


if __name__ == "__main__":
    unittest.main()

