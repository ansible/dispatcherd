from argparse import Namespace

from dispatcherd import cli


def test_set_log_level_schema_contains_required_level():
    schemas = cli.get_control_arg_schemas()
    assert 'set_log_level' in schemas
    level_schema = schemas['set_log_level']['level']
    assert level_schema['required'] is True
    assert set(level_schema['choices']) == {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}


def test_build_command_data_filters_fields_per_command():
    args = Namespace(level='INFO', task='demo_task', uuid='abc123')
    result = cli._build_command_data_from_args(args, 'set_log_level')
    assert result == {'level': 'INFO'}


def test_control_entrypoint_invokes_control(monkeypatch, capsys):
    captured = {}

    class DummyControl:
        def control_with_reply(self, command, data, expected_replies):
            captured['command'] = command
            captured['data'] = data
            captured['expected'] = expected_replies
            return [{'node_id': 'dummy', 'result': 'ok'}]

    monkeypatch.setattr(cli, 'get_control_from_settings', lambda: DummyControl())
    monkeypatch.setattr(cli, 'setup', lambda file_path=None: None)
    monkeypatch.setattr(cli.sys, 'argv', ['dispatcherctl', 'set_log_level', '--level', 'INFO'])

    cli.control()

    assert captured == {'command': 'set_log_level', 'data': {'level': 'INFO'}, 'expected': 1}
    output = capsys.readouterr().out
    assert 'result: ok' in output
