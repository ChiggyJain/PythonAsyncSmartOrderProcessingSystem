from typing import Any


class OrderValidationError(Exception):
    """
    Raised when the order payload is structurally valid JSON
    but fails business rules.
    """

    def __init__(self, message: str, details: Any | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class InventoryUnavailableError(Exception):
    """
    Raised when requested quantity cannot be satisfied from inventory.
    (We will use this later when we add inventory service.)
    """

    def __init__(self, sku: str, requested: int, available: int) -> None:
        super().__init__(
            f"Insufficient inventory for SKU={sku} "
            f"(requested={requested}, available={available})"
        )
        self.sku = sku
        self.requested = requested
        self.available = available
