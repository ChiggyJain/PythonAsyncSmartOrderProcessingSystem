import uuid
from datetime import datetime

def generate_order_id() -> str:
    """
    Generate an order ID that is:
    - globally unique
    - time-prefixed for easier debugging/partitioning
    Example: ORD-20241205-<uuid4>
    """
    now = datetime.utcnow().strftime("%Y%m%d")
    return f"ORD-{now}-{uuid.uuid4().hex[:12].upper()}"
