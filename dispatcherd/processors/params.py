from dataclasses import asdict, fields

from typing import Any


class ProcessorParams:
    def to_dict(self) -> dict[str,Any]:
        return asdict(self)

    @classmethod
    def from_message(cls, message: dict[str,Any]) -> 'ProcessorParams':
        reduced_data = {}
        for field in fields(cls):
            if field.name in message:
                reduced_data[field.name] = message[field.name]
        return cls(**reduced_data)
