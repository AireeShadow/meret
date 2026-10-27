from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class FileType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    UNKNOWN = "unknown"


class FileEntry(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    filename: str
    filepath: str
    labels: list[str] = Field(default_factory=list)
    rating: int = Field(default=0, ge=0, le=5)
    type: FileType = FileType.UNKNOWN
    metadata: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def add_label(self, label: str) -> None:
        if label not in self.labels:
            self.labels.append(label)

    def remove_label(self, label: str) -> None:
        if label in self.labels:
            self.labels.remove(label)


class Tag(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    name: str
    colour: str | None = None
    parent_id: int | None = None
    usage_count: int = 0


class TagNode(Tag):
    """Tag with children — for the tree (folders from tags)."""

    children: list["TagNode"] = Field(default_factory=list)


TagNode.model_rebuild()
