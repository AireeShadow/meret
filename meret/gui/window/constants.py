"""What the window is built from: the toolbar order and the sizes it uses.

Separate from both mixins on purpose. The toolbar is laid out by the layout mixin and
re-tinted by the settings code, so a constant living in either of them would make the
other import a module it has no business knowing.
"""

#: Toolbar icon size. Small enough that every command fits without Qt hiding one
#: behind its ">>" overflow button, which reads as a missing command.
ICON = 28

#: Toolbar buttons, in order. None is a separator. This is every command: a panel
#: that hides half of them is not "one to one with the TUI".
TOOLBAR: tuple = (
    "refresh",
    "scan",
    "search",
    None,
    "tag",
    "untag",
    "autotag",
    "delete",
    None,
    "sort_files",
    None,
    "init_db",
    "config",
    "stats",
    "tree",
    "prune",
    None,
    "mount",
    "umount",
    None,
    "command_palette",
    "help",
    None,
    "quit",
)

#: The help dialog groups these under Navigation, as the TUI's help does.
NAVIGATION_COMMANDS = ("sort_files",)
