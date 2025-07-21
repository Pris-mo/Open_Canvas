import pytest
from canvascrawler.client import Canvas
from unittest.mock import patch

@pytest.fixture
def dummy_client():
    return Canvas(token="abc", url="https://canvas.example.com")

@patch("canvascrawler.client.requests.get")
def test_get_course(mock_get, dummy_client):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"id": 42, "name": "Test"}
    data = dummy_client.get_course(course_id=42)
    assert data["id"] == 42
    mock_get.assert_called_once()
