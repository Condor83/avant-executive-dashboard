"""Response schemas for UI metadata endpoints."""

from pydantic import BaseModel

from api.schemas.common import OptionItem


class UiMetadataResponse(BaseModel):
    products: list[OptionItem]
    protocols: list[OptionItem]
    chains: list[OptionItem]
    wallets: list[OptionItem]
    position_sort_options: list[OptionItem]
    alert_severity_options: list[OptionItem]
    alert_status_options: list[OptionItem]
