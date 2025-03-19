import importlib
from types import ModuleType

from ..protocols import Broker


def get_broker_module(broker_name: str) -> ModuleType:
    "Static method to alias import_module so we use a consistent import path"
    return importlib.import_module(f'dispatcherd.brokers.{broker_name}')


def get_broker(broker_name: str, broker_config: dict, node_id: str, **overrides) -> Broker:
    """
    Given the name of the broker in the settings, and the data under that entry in settings,
    return the broker object.
    """
    broker_module = get_broker_module(broker_name)
    kwargs = broker_config.copy()
    kwargs.update(overrides)
    kwargs['node_id'] = node_id
    return broker_module.Broker(**kwargs)
