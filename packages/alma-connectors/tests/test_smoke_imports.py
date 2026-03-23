from importlib import import_module


def test_import_alma_connectors() -> None:
    module = import_module("alma_connectors")
    assert module is not None
