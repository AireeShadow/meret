# meret 🐍

Tagged media library: tags instead of folders, virtual hierarchy, GUI.

## Installation
pip install -e ".[dev]"

## Quick start
meret init ~/Media
meret scan ~/Media --db ~/Media
meret tag ~/Media/photo.jpg vacation beach
meret search --tag vacation
meret tree

## Debugging
Logs go to your user log folder (`meret logs` prints the path, `-n 50` the tail).

meret -v scan ~/Media              # DEBUG, also echoed to stderr
meret --log-level WARNING scan ... # quieter
MERET_LOG_LEVEL=DEBUG meret tui    # the TUI logs to the file only
MERET_LOG_FILE=./debug.log meret …  # somewhere else

At DEBUG every repository call is logged with its arguments, result size and time;
uncaught exceptions (CLI, TUI, threads, winfspy) land in the log with a traceback.
