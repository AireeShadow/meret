from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class FileType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    UNKNOWN = "unknown"


class FileEntry(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    filename: str
    filepath: str
    labels: List[str] = Field(default_factory=list)
    rating: int = Field(default=0, ge=0, le=5)
    type: FileType = FileType.UNKNOWN
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def add_label(self, label: str) -> None:
        if label not in self.labels:
            self.labels.append(label)

    def remove_label(self, label: str) -> None:
        if label in self.labels:
            self.labels.remove(label)


class Tag(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    name: str
    colour: Optional[str] = None
    parent_id: Optional[int] = None
    usage_count: int = 0


class TagNode(Tag):
    """Тег с детьми — для дерева (папки из тегов)."""
    children: List["TagNode"] = Field(default_factory=list)


TagNode.model_rebuild()