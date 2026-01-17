def build_coverage_probe_response(token: str | None) -> dict[str, str]:
    """Return a deterministic payload for coverage probing."""
    return {
        'probe': 'ok',
        'token': token or 'unset',
    }
