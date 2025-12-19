import importlib
from types import ModuleType

from ..protocols import Broker


def _resolve_module_name(broker_name: str) -> str:
    """
    A broker entry can either be a short name (e.g. ``pg_notify``) or a dotted module path.

    Short names are assumed to live under ``dispatcherd.brokers`` for backwards compatibility.
    """
    if '.' in broker_name:
        return broker_name
    return f'dispatcherd.brokers.{broker_name}'


def get_broker_module(broker_name: str) -> ModuleType:
    """Import the Python module that defines the broker."""
    module_name = _resolve_module_name(broker_name)
    return importlib.import_module(module_name)


def get_broker(broker_name: str, broker_config: dict, **overrides) -> Broker:  # type: ignore[no-untyped-def]
    """
    Given the name of the broker in the settings, and the data under that entry in settings,
    return the broker object.
    """
    broker_module = get_broker_module(broker_name)
    kwargs = broker_config.copy()
    kwargs.update(overrides)
    return broker_module.Broker(**kwargs)
