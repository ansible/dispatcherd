import time

import pytest

from dispatcherd.registry import DispatcherMethod, InvalidMethod, UnregisteredMethod
from tests.data.nested.nested_registry import surprised_registry


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


def test_surprise_registration():
    """Finds a registered method in registry

    where the method is not registered until the lookup happens
    this is important so that if the user does not pre-load the registry
    at import time, the parameters of registered methods are still respected
    """
    assert len(surprised_registry.lookup_dict) == 0
    dmethod = surprised_registry.get_method('tests.data.nested.methods.print_hello')
    assert len(surprised_registry.registry) == 1  # we grew!
    assert len(surprised_registry.lookup_dict) == 1
    assert isinstance(dmethod, DispatcherMethod)
    assert not isinstance(dmethod, UnregisteredMethod)


def test_get_async_body_rejects_invalid_uuid4(registry):
    """Invalid uuid4 inputs must raise rather than being silently replaced."""

    def test_method():
        return

    dmethod = registry.register(test_method)

    with pytest.raises(ValueError, match="Invalid UUID4 format"):
        dmethod.get_async_body(uuid="not-a-uuid")

    with pytest.raises(ValueError, match="Invalid UUID4 format"):
        dmethod.get_async_body(uuid="1234")

    # Non-canonical (uppercase) form is rejected — AWX uses uuid4() lowercase output
    good = "550e8400-e29b-41d4-a716-446655440000"
    with pytest.raises(ValueError, match="Invalid UUID4 format"):
        dmethod.get_async_body(uuid=good.upper())


def test_get_async_body_accepts_canonical_uuid4(registry):
    def test_method():
        return

    dmethod = registry.register(test_method)
    good = "550e8400-e29b-41d4-a716-446655440000"
    body = dmethod.get_async_body(uuid=good)
    assert body["uuid"] == good


def test_get_async_body_generates_uuid_when_omitted(registry):
    def test_method():
        return

    dmethod = registry.register(test_method)
    body = dmethod.get_async_body()
    # Generated values are canonical uuid4 strings
    from uuid import UUID

    UUID(body["uuid"], version=4)
    assert body["uuid"] == str(UUID(body["uuid"], version=4))

