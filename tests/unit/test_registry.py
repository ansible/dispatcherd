import time

import pytest

from dispatcher.registry import InvalidMethod


def test_registry_ordinary_method(registry):
    def test_method():
        return
    registry.register(test_method)
    assert test_method in set(dmethod.fn for dmethod in registry.registry)
    assert 'test_registry.test_registry_ordinary_method.<locals>.test_method' in registry.lookup_dict
    assert len(registry.registry) == 1


def test_register_class(registry):
    class SomeClass:
        def run(self):
            return

    registry.register(SomeClass)
    assert SomeClass in set(dmethod.fn for dmethod in registry.registry)
    assert 'test_registry.test_register_class.<locals>.SomeClass' in registry.lookup_dict
    assert len(registry.registry) == 1


def test_no_objects(registry):
    class SomeClass:
        def run(self):
            return

    with pytest.raises(InvalidMethod):
        registry.register(SomeClass())


def test_register_with_timeout(registry):
    "Tests that a timeout set at the task level will be submitted"
    def test_method():
        time.sleep(4)  # will not actually run

    dmethod = registry.register(test_method, timeout=0.2)
    submit_data = dmethod.get_async_body()
    assert submit_data['timeout'] == 0.2
