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
    label_group: Optional[str] = None
    label_subgroup: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def add_label(self, label: str) -> None:
        """Добавить тег, если его ещё нет"""
        if label not in self.labels:
            self.labels.append(label)
    
    def remove_label(self, label: str) -> None:
        """Удалить тег"""
        if label in self.labels:
            self.labels.remove(label)


class Tag(BaseModel):
    name: str
    description: Optional[str] = None
    usage_count: int = 0
    colour: Optional[str] = None  # Например, "#FF5733"


class TagGroup(BaseModel):
    name: str
    description: Optional[str] = None
    subgroups: List[str] = Field(default_factory=list)