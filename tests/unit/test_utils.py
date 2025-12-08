import pytest

from dispatcherd.utils import resolve_callable


def test_resolve_lamda_method():
    method = resolve_callable('lambda: 45')
    assert method() == 45


def test_resolve_callable_invalid():
    with pytest.raises(RuntimeError):
        resolve_callable('notamethod')
