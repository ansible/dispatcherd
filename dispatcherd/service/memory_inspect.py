import gc
import sys
from typing import NotRequired, TypedDict


class _Offender(TypedDict):
    count: int
    size_bytes: int
    type: str
    class_: NotRequired[str]
    module: NotRequired[str]
    qualname: NotRequired[str]


def _class_identifier(obj: object) -> tuple[str, str, str]:
    type_obj = type(obj)
    module = getattr(type_obj, '__module__', 'builtins')
    qualname = getattr(type_obj, '__qualname__', type_obj.__name__)
    return module, qualname, type_obj.__name__


def get_object_size_stats(limit: int = 10, group_by: str = 'type') -> dict[str, object]:
    """Return shallow size totals grouped by object type or class."""
    objects = gc.get_objects()
    totals: dict[str, _Offender] = {}
    by_class = group_by == 'class'

    for obj in objects:
        module, qualname, type_name = _class_identifier(obj)
        if by_class:
            key = f'{module}.{qualname}'
        else:
            key = type_name

        try:
            size = sys.getsizeof(obj)
        except Exception:
            continue

        entry = totals.get(key)
        if entry is None:
            entry = {'count': 0, 'size_bytes': 0, 'type': type_name}
            if by_class:
                entry['module'] = module
                entry['qualname'] = qualname
                entry['class_'] = key
            else:
                entry['type'] = type_name
            totals[key] = entry

        entry['count'] += 1
        entry['size_bytes'] += size

    offenders = []
    for entry in totals.values():
        if 'class_' in entry:
            trimmed = {key: value for key, value in entry.items() if key != 'class_'}
            trimmed['class'] = entry['class_']
            offenders.append(trimmed)
        else:
            offenders.append(dict(entry))
    offenders.sort(key=lambda item: (item['size_bytes'], item['count'], item['type']), reverse=True)

    return {
        'total_objects': len(objects),
        'group_by': 'class' if by_class else 'type',
        'offenders': offenders[: max(limit, 0)],
    }
