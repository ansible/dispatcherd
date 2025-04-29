from dataclasses import dataclass, fields

from dispatcherd.processors.params import ProcessorParams


def test_save_and_load_params():
    @dataclass(kw_only=True)
    class MyParams(ProcessorParams):
        foo: int = 3
        bar: str

    params = MyParams(foo=1, bar='foobar')
    assert set(f.name for f in fields(params)) == {'foo', 'bar'}
    obj = params.from_message({"foo": 3, "bar": "not_foobar", "other_key": "do_not_incluede"})
    assert obj.to_dict() == {"foo": 3, "bar": "not_foobar"}
