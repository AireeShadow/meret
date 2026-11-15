"""The window, split by what it is responsible for.

`MainWindow` is assembled from mixins instead of grown as one module: the browser,
the commands, the database switch and the shell-outs each change for their own
reasons, and a 1900-line file makes all of them look like the same change.

A mixin is a plain class with no state of its own. Everything it touches is created
by `WindowBase.__init__`, which is why the split needs no constructor arguments and
no wiring: the attributes are the contract.
"""
