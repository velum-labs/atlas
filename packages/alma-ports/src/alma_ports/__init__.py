"""alma-ports — Protocol interfaces for Alma Atlas.

Zero-dependency package that defines the core port protocols used across all
Alma Atlas packages. All adapters and implementations depend on these interfaces,
never on each other directly.
"""

__version__ = "0.1.0"

from alma_ports.asset import AssetPort
from alma_ports.consumer import ConsumerPort
from alma_ports.contract import ContractPort
from alma_ports.edge import EdgePort
from alma_ports.query import QueryPort
from alma_ports.schema import SchemaPort

__all__ = [
    "AssetPort",
    "ConsumerPort",
    "ContractPort",
    "EdgePort",
    "QueryPort",
    "SchemaPort",
]
