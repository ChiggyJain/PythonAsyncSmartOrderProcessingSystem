from typing import Tuple
from app.core.exceptions import OrderValidationError
from app.schemas.order_schema import OrderCreate


# Example allowed max order value in INR
MAX_ORDER_AMOUNT_INR = 10_00_000.0 


def _convert_to_inr(amount: float, currency: str) -> float:
    """
    Simple, hard-coded FX conversion for demo purposes.
    In a real system this would call an FX service.
    """
    rates = {
        "INR": 1.0,
        "USD": 83.0,
        "EUR": 90.0,
    }
    return amount * rates.get(currency, 1.0)


async def validate_order_business_rules(order: OrderCreate) -> Tuple[float, float]:
    """
    Async business validation for an order.
    Returns:
        tuple: (total_amount_in_order_currency, total_amount_in_inr)
    Raises:
        OrderValidationError: if any rule fails
    """
    if not order.items:
        raise OrderValidationError("Order must contain at least one item")
    total_amount = 0.0
    for item in order.items:
        line_total = item.quantity * item.unit_price
        if line_total <= 0:
            raise OrderValidationError(
                "Line total must be positive",
                details={"sku": item.sku, "line_total": line_total},
            )
        total_amount += line_total
    total_amount_inr = _convert_to_inr(total_amount, order.currency)
    if total_amount_inr > MAX_ORDER_AMOUNT_INR:
        raise OrderValidationError(
            "Order amount exceeds allowed limit",
            details={
                "total_amount": total_amount,
                "total_amount_inr": total_amount_inr,
                "limit_inr": MAX_ORDER_AMOUNT_INR,
            },
        )
    # Add more rules later (blacklisted customers, velocity checks, etc.)
    return total_amount, total_amount_inr
