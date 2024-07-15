""""
 For licensing see accompanying LICENSE file.
 Copyright (C) 2024 Apple Inc. All Rights Reserved.
"""

import requests
import unittest
from unittest import mock
from pathlib import Path

import apple_music_feed_example


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


mocked_requests_send = [
    MockResponse(
        {
            "next": "/v1/feed/exports/example/parts?limit=2&offset=2",
            "resources": {
                "parts": {
                    "1": {"id": "1", "attributes": {"exportLocation": "location1"}},
                    "2": {"id": "2", "attributes": {"exportLocation": "location2"}},
                }
            },
        },
        200,
    ),
    MockResponse(
        {
            "resources": {
                "parts": {
                    "3": {"id": "3", "attributes": {"exportLocation": "location3"}},
                    "4": {"id": "4", "attributes": {"exportLocation": "location4"}},
                }
            }
        },
        200,
    ),
]


class TestAppleMusicFeedExample(unittest.TestCase):
    @mock.patch("requests.Session.send", side_effect=mocked_requests_send)
    def test_get_feed_urls(self, mock_send):
        result = apple_music_feed_example.get_feed_urls(
            "example",
            "example",
            "https://api.media.apple.com",
            requests.Session(),
            "jwt",
            Path("fake_doesnt_exist"),
        )
        print(result)
        self.assertEqual(4, len(result))
        self.assertEqual(
            {
                apple_music_feed_example.PartLocation(
                    Path("fake_doesnt_exist/4"), "location4"
                ),
                apple_music_feed_example.PartLocation(
                    Path("fake_doesnt_exist/3"), "location3"
                ),
                apple_music_feed_example.PartLocation(
                    Path("fake_doesnt_exist/2"), "location2"
                ),
                apple_music_feed_example.PartLocation(
                    Path("fake_doesnt_exist/1"), "location1"
                ),
            },
            result,
        )


if __name__ == "__main__":
    unittest.main()
