"""The `[detector]` section of a rules file.

A library's policy is one file: which model to ask, how sure it has to be, and what to
tag. Keeping the checkpoint next to the rules means the two cannot disagree — a rule
over `detect.cat` and a model that has never heard of cats is a typo nobody would find
if the two lived in different files.

No `[detector]` section means no detection pass. A library that was tagged by hand must
not start downloading weights because a rules file happens to exist.
"""

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from meret.core.detectors.base import Detector, create_detector

logger = logging.getLogger(__name__)

DEFAULT_BACKEND = "yolo"
DEFAULT_MODEL = "yolo26n.pt"


class DetectorPolicy(BaseModel):
    """What to run and how. `backend` names a registered detector and nothing else."""

    backend: str = Field(default=DEFAULT_BACKEND, description="Registered detector name")
    model: str = Field(default=DEFAULT_MODEL, description="Checkpoint path or name")
    confidence: float = Field(default=0.25, ge=0.0, le=1.0)
    imgsz: int = Field(default=640, ge=32)
    device: str | None = Field(default=None, description="cpu, cuda:0, mps; None lets it choose")
    classes: list[str] | None = Field(
        default=None, description="Labels to keep; None keeps every label the model knows"
    )

    def build(self) -> Detector:
        """The detector this policy describes, imported on first use."""
        return create_detector(
            self.backend,
            model=self.model,
            confidence=self.confidence,
            imgsz=self.imgsz,
            device=self.device,
            classes=self.classes,
        )


def load_policy(path: Path) -> DetectorPolicy | None:
    """The `[detector]` table of a rules file, or None when the file has none.

    Accepted shapes match the rule loader: a bare `[detector]` table, or the same
    table nested under `[autotag]`. A JSON file works the same way.
    """
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data: Any = json.loads(text)
    else:
        import tomllib

        data = tomllib.loads(text)

    if not isinstance(data, dict):
        return None

    section = data.get("detector")
    if section is None:
        nested = data.get("autotag")
        section = nested.get("detector") if isinstance(nested, dict) else None
    if not isinstance(section, dict):
        return None
    return DetectorPolicy.model_validate(section)
