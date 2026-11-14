"""Debug switches: one list of names, one command-line flag.

Every diagnostic that used to be an environment variable is a name here, and
`meret --debug watchdog,watch_fs gui` turns them on. The environment is still read
as a fallback, so an existing script keeps working; the flag wins when both are
present, and a name that does not exist is a command-line error instead of a silent
no-op - which is exactly what a misspelled environment variable always was.
"""

import logging
import os
from collections.abc import Iterable

logger = logging.getLogger(__name__)

#: Debug name -> the environment variable it used to be.
#: The order here is the order the help text lists them in.
FLAGS: dict[str, str] = {
    "watchdog": "MERET_WATCHDOG",
    "watch_fs": "MERET_WATCH_FS",
    "settings_modeless": "MERET_SETTINGS_MODELESS",
    "media_log": "MERET_MEDIA_LOG",
    "no_tooltips": "MERET_NO_TOOLTIPS",
}

_active: frozenset[str] = frozenset()


def names() -> tuple[str, ...]:
    """Every debug name, for the help text."""
    return tuple(FLAGS)


def parse(value: str | None) -> frozenset[str]:
    """Names from a comma-separated list.

    Hyphens are accepted as well as underscores: a flag typed by hand tends to come
    out as `settings-modeless`, and refusing that would be pedantry, not validation.
    """
    if not value:
        return frozenset()
    wanted = {part.strip().lower().replace("-", "_") for part in value.split(",") if part.strip()}
    unknown = sorted(wanted - set(FLAGS))
    if unknown:
        raise ValueError(
            f"unknown debug flag(s): {', '.join(unknown)}; available: {', '.join(FLAGS)}"
        )
    return frozenset(wanted)


def set_enabled(flags: Iterable[str]) -> None:
    """Replace the active set. Called once, from the CLI callback."""
    global _active
    _active = frozenset(flags)
    if _active:
        logger.info("debug flags: %s", ", ".join(sorted(_active)))


def enabled(name: str) -> bool:
    """True when the flag was asked for on the command line or in the environment."""
    if name in _active:
        return True
    variable = FLAGS.get(name)
    return bool(variable and os.environ.get(variable))
