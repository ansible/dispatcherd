from dataclasses import asdict, dataclass, fields
from typing import Any, TypeVar

T = TypeVar("T", bound="ProcessorParams")


@dataclass(kw_only=True)
class ProcessorParams:
    """Data structure for task options, specific to a particular processor in the dispatcherd service"""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_message(cls: type[T], message: dict[str, Any]) -> T:
        """Used by the dispatcherd service, given a message from the broker, return the params for associated processor"""
        reduced_data = {}
        for field in fields(cls):
            if field.name in message:
                reduced_data[field.name] = message[field.name]
        return cls(**reduced_data)
