"""Ошибки виртуальной ФС."""


class VfsError(Exception):
    """Базовая ошибка виртуальной ФС."""


class NotFound(VfsError):
    """Узел не найден."""


class NotADirectory(VfsError):
    """Ожидался каталог, а это файл."""


class IsADirectory(VfsError):
    """Ожидался файл, а это каталог."""