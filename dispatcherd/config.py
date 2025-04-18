import os
from contextlib import contextmanager
from typing import Optional

import yaml


class DispatcherSettings:
    def __init__(self, config: dict) -> None:
        self.version = 2
        if config.get('version') != self.version:
            raise RuntimeError(f'Current config version is {self.version}, config version must match this')
        self.brokers: dict = config.get('brokers', {})
        self.producers: dict = config.get('producers', {})
        self.service: dict = config.get('service', {})
        self.publish: dict = config.get('publish', {})
        self.worker: dict = config.get('worker', {})

        # Automatic defaults
        if 'pool_kwargs' not in self.service:
            self.service['pool_kwargs'] = {}
        if 'max_workers' not in self.service['pool_kwargs']:
            self.service['pool_kwargs']['max_workers'] = 3

    def serialize(self):
        return dict(version=self.version, brokers=self.brokers, producers=self.producers, service=self.service, publish=self.publish, worker=self.worker)


def settings_from_file(path: str) -> DispatcherSettings:
    with open(path, 'r') as f:
        config_content = f.read()

    config = yaml.safe_load(config_content)
    return DispatcherSettings(config)


def settings_from_env() -> DispatcherSettings:
    if file_path := os.getenv('DISPATCHERD_CONFIG_FILE'):
        return settings_from_file(file_path)
    raise RuntimeError('Dispatcherd not configured, set DISPATCHERD_CONFIG_FILE or call dispatcherd.config.setup')


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


def setup(config: Optional[dict] = None, file_path: Optional[str] = None) -> LazySettings:
    if config:
        settings._wrapped = DispatcherSettings(config)
    elif file_path:
        settings._wrapped = settings_from_file(file_path)
    else:
        settings._wrapped = settings_from_env()
    return settings


@contextmanager
def temporary_settings(config):
    prior_settings = settings._wrapped
    try:
        settings._wrapped = DispatcherSettings(config)
        yield settings
    finally:
        settings._wrapped = prior_settings


def is_setup(for_settings: LazySettings = settings) -> bool:
    """Tells whether dispatcherd has been configured"""
    return bool(for_settings._wrapped is not None)
