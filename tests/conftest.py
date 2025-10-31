import pytest

@pytest.fixture
def sample_order():
    return {
        "order_id": "TEST123",
        "timestamp": "2024-01-01T00:00:00Z",
        "total_amount": 99.99
    }
