"""Submodule that contains typing for SDK responses and requests"""

from __future__ import annotations
from typing import Any, TypedDict, Literal

Action = Literal['insert', 'update', 'delete']
ISOTimestamp = str
IPAddress = str
UserAgent = str

class Activity(TypedDict, total=False):
    id: int
    action: Action
    timestamp: ISOTimestamp
    ip: IPAddress
    user_agent: UserAgent
    item: int
    comment: str | None
    origin: str
    revisions: list[Any]