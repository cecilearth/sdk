from typing import Optional

from .base import Base


class Usage(Base):
    num_subscriptions: int
    monthly_subscriptions: int
    total_area_ha: float
    monthly_area_ha: float
    num_aois: int
    monthly_aois: int
    monthly_subscription_limit: float
    max_subscriptions: Optional[int] = None
    max_total_area_ha: Optional[float] = None
