"""The YOLO backend: an Ultralytics checkpoint, loaded once per pass.

`ultralytics` is imported inside `_load`, so `core` stays importable without torch —
the same rule the GUI follows for PySide6. Someone who never runs a detector never
installs two gigabytes of wheels.

The class names are not configured anywhere: `result.names` comes from the checkpoint
itself, so a custom `best.pt` with three classes of your own needs no code and no
config at all.
"""

import logging
from pathlib import Path
from typing import Any
import importlib.util

from meret.core.detectors.base import Detection, register

logger = logging.getLogger(__name__)

#: Below this a detection is noise for most libraries. A rule can still be stricter.
DEFAULT_CONFIDENCE = 0.25
DEFAULT_IMGSZ = 640


class YoloDetector:
    """One Ultralytics checkpoint, reused for a whole pass.

    Loading is lazy and happens on the first file: `YOLO(...)` reads weights and
    touches torch, and a pass that turns out to have nothing to do should not pay for
    either.
    """

    def __init__(
        self,
        model: str | Path,
        confidence: float = DEFAULT_CONFIDENCE,
        imgsz: int = DEFAULT_IMGSZ,
        device: str | None = None,
        classes: list[str] | None = None,
        name: str | None = None,
    ) -> None:
        if importlib.util.find_spec("ultralytics") is None:
            raise ImportError("the 'ultralytics' package is not installed")

        self.model_path = str(model)
        self.confidence = confidence
        self.imgsz = imgsz
        self.device = device
        self.wanted = {label.casefold() for label in classes} if classes else None
        self.name = name or Path(self.model_path).stem
        self._model: Any = None

    def _load(self) -> Any:
        if self._model is None:
            from ultralytics import YOLO

            logger.info("loading detector %s from %s", self.name, self.model_path)
            self._model = YOLO(self.model_path)
        return self._model

    def detect(self, path: str) -> list[Detection] | None:
        try:
            results = self._load().predict(
                source=path,
                conf=self.confidence,
                imgsz=self.imgsz,
                device=self.device,
                verbose=False,
            )
        except Exception as e:
            logger.warning("detector %s could not read %s: %s", self.name, path, e)
            return None

        found: list[Detection] = []
        for result in results:
            boxes = result.boxes
            if boxes is None:
                continue
            names = result.names or {}
            for index, class_id in enumerate(boxes.cls.tolist()):
                label = str(names.get(int(class_id), int(class_id)))
                if self.wanted is not None and label.casefold() not in self.wanted:
                    continue
                x1, y1, x2, y2 = (float(value) for value in boxes.xyxy[index].tolist())
                found.append(
                    Detection(
                        label=label,
                        confidence=float(boxes.conf[index]),
                        box=(x1, y1, x2, y2),
                    )
                )
        return found


def factory(
    model: str | Path,
    confidence: float = DEFAULT_CONFIDENCE,
    imgsz: int = DEFAULT_IMGSZ,
    device: str | None = None,
    classes: list[str] | None = None,
    **_: object,
) -> YoloDetector:
    """Factory for `create_detector("yolo", ...)`.

    Extra keywords are ignored, so one rules file may carry options that only another
    backend understands without breaking this one.
    """
    return YoloDetector(model, confidence, imgsz, device, classes)


register("yolo", factory)
