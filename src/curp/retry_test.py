"""
Retry test
"""

from datetime import timedelta
from src.curp.retry import RetryConfig


def test_fixed_backoff_works():
    """
    Test fixed backoff works
    """
    config = RetryConfig.new_fixed(timedelta(seconds=1), 3)
    backoff = config.init_backoff()
    assert backoff.next_delay() == timedelta(seconds=1)
    assert backoff.next_delay() == timedelta(seconds=1)
    assert backoff.next_delay() == timedelta(seconds=1)
    assert backoff.next_delay() is None


def test_exponential_backoff_works():
    """
    TestExponentialBackoffWorks
    """
    config = RetryConfig.new_exponential(timedelta(seconds=1), timedelta(seconds=5), 4)
    backoff = config.init_backoff()
    assert backoff.next_delay() == timedelta(seconds=1)
    assert backoff.next_delay() == timedelta(seconds=2)
    assert backoff.next_delay() == timedelta(seconds=4)
    assert backoff.next_delay() == timedelta(seconds=5) # 8 > 5
    assert backoff.next_delay() is None
