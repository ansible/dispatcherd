import pytest

from ansible_dispatcher.registry import DispatcherMethodRegistry


@pytest.fixture
def registry() -> DispatcherMethodRegistry:
    "Return a fresh registry, separate from the global one, for testing"
    return DispatcherMethodRegistry()
