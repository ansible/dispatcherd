import os
from contextlib import contextmanager
from typing import Optional

import yaml


class DispatcherSettings:
    def __init__(self, config: dict) -> None:
        self.brokers: dict = config.get('brokers', {})
        self.producers: dict = config.get('producers', {})
        self.service: dict = config.get('service', {'max_workers': 3})
        self.publish: dict = config.get('publish', {})
        # TODO: firmly planned sections of config for later
        # self.callbacks: dict = config.get('callbacks', {})
        # self.options: dict = config.get('options', {})

    def serialize(self):
        return dict(
            brokers=self.brokers,
            producers=self.producers,
            service=self.service,
            publish=self.publish
        )


def settings_from_file(path: str) -> DispatcherSettings:
    with open(path, 'r') as f:
        config_content = f.read()

    config = yaml.safe_load(config_content)
    return DispatcherSettings(config)


def settings_from_env() -> DispatcherSettings:
    if file_path := os.getenv('DISPATCHER_CONFIG_FILE'):
        return settings_from_file(file_path)
    raise RuntimeError('Dispatcher not configured, set DISPATCHER_CONFIG_FILE or call dispatcher.config.setup')


class LazySettings:
    def __init__(self) -> None:
        self._wrapped: Optional[DispatcherSettings] = None

    def __getattr__(self, name):
        if self._wrapped is None:
            self._setup()
        return getattr(self._wrapped, name)

    def _setup(self) -> None:
        self._wrapped = settings_from_env()


settings = LazySettings()


def setup(config: Optional[dict] = None, file_path: Optional[str] = None):
    if config:
        settings._wrapped = DispatcherSettings(config)
    elif file_path:
        settings._wrapped = settings_from_file(file_path)
    else:
        settings._wrapped = settings_from_env()


@contextmanager
def temporary_settings(config):
    prior_settings = settings._wrapped
    try:
        settings._wrapped = DispatcherSettings(config)
        yield settings
    finally:
        settings._wrapped = prior_settings
