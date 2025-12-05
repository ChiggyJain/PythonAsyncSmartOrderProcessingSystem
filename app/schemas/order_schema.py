from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator
from pydantic_core import PydanticUndefined

# its like enum form
# means anyone value
OrderStatus = Literal[
    "received",    # just accepted by API
    "validated",   # passed validation
    "enriched",    # inventory/pricing attached
    "dispatched",  # sent to Kafka
    "failed",      # validation or processing error
]


class OrderItem(BaseModel):
    sku: str = Field(..., min_length=1, max_length=64, description="Stock Keeping Unit")
    name: str = Field(..., min_length=1, max_length=255)
    quantity: int = Field(..., gt=0, le=10_000)
    unit_price: float = Field(..., gt=0)

    # added vaalidator sku field only.
    # returning data into uppercase form only
    @field_validator("sku")
    @classmethod
    def normalize_sku(cls, v: str) -> str:
        return v.strip().upper()

    # added vaalidator name field only.
    # returning data with removing extra spaces
    @field_validator("name")
    @classmethod
    def normalize_name(cls, v: str) -> str:
        return v.strip()


class OrderCreate(BaseModel):
    customer_id: str = Field(..., min_length=1, max_length=64)
    currency: Literal["INR", "USD", "EUR"] = "INR"
    items: list[OrderItem] = Field(..., min_length=1)
    source: Literal["web", "mobile", "partner_api"] = "web"
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # added vaalidator customer_id field only.
    # returning data with removing extra spaces
    @field_validator("customer_id")
    @classmethod
    def normalize_customer_id(cls, v: str) -> str:
        return v.strip()

    @field_validator("created_at", mode="before")
    @classmethod
    def default_created_at(cls, v) -> datetime:
        if v is None or v is PydanticUndefined:
            return datetime.utcnow()
        return v

    @field_validator("items")
    @classmethod
    def validate_total_items(cls, items: list[OrderItem]) -> list[OrderItem]:
        if len(items) > 1_000:
            raise ValueError("Too many items in a single order")
        return items

# this class is defined for outputing the order response
class OrderPublic(BaseModel):
    """
    What we return to API consumers after order creation.
    """
    order_id: str
    status: OrderStatus
    customer_id: str
    currency: str
    total_amount: float
    created_at: datetime
    source: str
