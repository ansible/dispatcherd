import pytest

from dispatcherd.utils import is_valid_uuid, resolve_callable


def test_resolve_lamda_method():
    method = resolve_callable('lambda: 45')
    assert method() == 45


def test_resolve_callable_invalid():
    with pytest.raises(RuntimeError):
        resolve_callable('notamethod')


def test_is_valid_uuid():
    """Test UUID validation with valid UUIDs."""
    assert is_valid_uuid('550e8400-e29b-41d4-a716-446655440000')
    assert is_valid_uuid('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    assert is_valid_uuid('6BA7B810-9DAD-11D1-80B4-00C04FD430C8')  # uppercase


def test_is_valid_uuid_invalid():
    """Test UUID validation with invalid UUIDs."""
    assert not is_valid_uuid('not-a-uuid')
    assert not is_valid_uuid('550e8400-e29b-41d4-a716')  # too short
    assert not is_valid_uuid('550e8400-e29b-41d4-a716-446655440000-extra')  # too long
    assert not is_valid_uuid('')  # empty string
