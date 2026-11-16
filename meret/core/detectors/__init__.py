"""Detectors: what found something in a file, independent of which model did."""

from meret.core.detectors.base import (
    DETECT_KEY,
    Detection,
    Detector,
    create_detector,
    facts,
    known_backends,
    register,
)

__all__ = [
    "DETECT_KEY",
    "Detection",
    "Detector",
    "create_detector",
    "facts",
    "known_backends",
    "register",
]
