import pytest

from dispatcher.registry import DispatcherMethodRegistry


@pytest.fixture
def registry():
    "Return a fresh registry, separate from the global one, for testing"
    return DispatcherMethodRegistry()


def test_registry_ordinary_method(registry):
    def test_method():
        return
    registry.register(test_method)
    assert test_method in set(dmethod.fn for dmethod in registry.registry)
    assert 'test_registry:test_registry_ordinary_method.<locals>.test_method' in registry.lookup_dict
    assert len(registry.registry) == 1


def test_register_class(registry):
    class SomeClass:
        def run(self):
            return

    registry.register(SomeClass)
    assert SomeClass in set(dmethod.fn for dmethod in registry.registry)
    assert 'test_registry:test_register_class.<locals>.SomeClass' in registry.lookup_dict
    assert len(registry.registry) == 1
