"""What a detector is, independent of which model answers.

`core` never imports a model library: a detector is anything that turns one file into
a list of `Detection`s and is registered under a name. The YOLO backend is one
implementation and lives in `meret/core/detectors/yolo.py`, which imports
`ultralytics` inside the function that needs it.

Detections reach the rule engine as metadata, not as a new operator. `facts()` writes
flat keys under `detect`, and the existing fact builder already flattens one level of
a nested dict, so `detect.cat` is an ordinary fact and the rule language needs no
knowledge of models at all.
"""

import importlib
import logging
from collections.abc import Callable, Iterable
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

#: Metadata key the detections are stored under, and the prefix of every fact.
DETECT_KEY = "detect"


class Detection(BaseModel):
    """One thing a model found in one file.

    `label` is whatever the model calls it — a COCO name (`person`), a class of a
    custom checkpoint (`crack`), anything. `confidence` is the model's own score and is
    kept on purpose: a rule decides where the line is instead of trusting the model's
    default threshold.
    """

    label: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    box: tuple[float, float, float, float] | None = None


@runtime_checkable
class Detector(Protocol):
    """A named source of detections.

    `name` goes into the stored block, so a library tagged by two models can be told
    apart — and a re-run knows whether the boxes on disk came from the checkpoint that
    is configured now.
    """

    name: str

    def detect(self, path: str) -> list[Detection] | None:
        """Every object this model found in `path`, above its own threshold.

        An empty list is an answer: the model looked and found nothing, and that answer
        is worth caching. None is the absence of an answer — an unreadable file, a
        decoder that gave up, a GPU out of memory — and the two must not be confused,
        because only one of them is worth asking again.
        """
        ...


#: Backend name -> factory. `register` is the only way in, so a missing backend is a
#: clear "unknown detector" instead of an ImportError three frames down.
_FACTORIES: dict[str, Callable[..., Detector]] = {}

#: Backends that ship with meret, imported only when someone asks for one by name.
_BUILTIN: dict[str, str] = {"yolo": "meret.core.detectors.yolo"}


def register(name: str, factory: Callable[..., Detector]) -> None:
    """Make a backend available under `name` (the value of `[detector] backend`)."""
    _FACTORIES[name.casefold()] = factory


def known_backends() -> list[str]:
    """Every registered backend name, sorted — what an error message lists."""
    return sorted(set(_FACTORIES) | set(_BUILTIN))


def create_detector(backend: str, **options: object) -> Detector:
    """Build the detector named `backend`, passing `options` to its factory.

    A built-in backend is imported on first use, which is what keeps `ultralytics` out
    of the import graph of everything that merely has a rules file.
    """
    key = backend.casefold()
    if key not in _FACTORIES and key in _BUILTIN:
        importlib.import_module(_BUILTIN[key])
    if key not in _FACTORIES:
        raise ValueError(
            f"unknown detector backend {backend!r}; known: {', '.join(known_backends())}"
        )
    return _FACTORIES[key](**options)


def facts(
    detections: Iterable[Detection],
    model: str = "",
    stamp: object = None,
) -> dict[str, object]:
    """Detections as flat metadata keys, ready for the fact builder to flatten.

    Three keys per label, because they answer three different questions:

    - `cat`         the best confidence for that label — "is there a cat, how sure"
    - `cat.count`   how many — "one cat" and "a crowd of cats" are different tags
    - `classes`     the labels as one comma-separated string, so `contains` works

    Count and confidence stay apart on purpose: a rule that wants "at least one
    confident cat" writes one condition, not a loop over boxes. `model` and `mtime`
    are the staleness record: they say who answered and about which version of the file.
    """
    best: dict[str, float] = {}
    counts: dict[str, int] = {}
    for detection in detections:
        label = detection.label.strip()
        if not label:
            continue
        counts[label] = counts.get(label, 0) + 1
        best[label] = max(best.get(label, 0.0), detection.confidence)

    out: dict[str, object] = {"model": model, "mtime": stamp}
    for label in sorted(best):
        out[label] = round(best[label], 3)
        out[f"{label}.count"] = counts[label]
    out["classes"] = ",".join(sorted(counts))
    return out
