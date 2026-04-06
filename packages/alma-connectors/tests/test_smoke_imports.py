from importlib import import_module

import alma_connectors


def test_import_alma_connectors() -> None:
    module = import_module("alma_connectors")
    assert module is not None


def test_connectors_version_accessible() -> None:
    assert hasattr(alma_connectors, "__version__")
    assert isinstance(alma_connectors.__version__, str)
    assert alma_connectors.__version__  # non-empty
