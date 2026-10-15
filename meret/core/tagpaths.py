"""Пути тегов: «жанр/детектив».

В БД теги плоские (id, name, parent_id), иерархию задаёт parent_id.
Здесь она превращается в путь и обратно — больше нигде этого не делаем.

Почему путь: CLI и виртуальная ФС показывают иерархию папками,
и «жанр/детектив» однозначнее, чем «перевесь детектив под жанр».

Имена тегов глобально уникальны, поэтому resolve принимает и полный путь,
и просто имя: после `parent b a` путь тега `b` становится `a/b`, и по имени
его всё равно надо находить.
"""

from typing import Callable, Dict, Iterable, List, Optional, Tuple

from meret.core.models import Tag

SEP = "/"


def normalize(path: str) -> str:
    """' /жанр//детектив/ ' → 'жанр/детектив'."""
    return SEP.join(p.strip() for p in path.split(SEP) if p.strip())


def parts(path: str) -> List[str]:
    norm = normalize(path)
    return norm.split(SEP) if norm else []


def index_by_parent(
    tags: Iterable[Tag], key: Callable[[Tag], str] = lambda t: t.name.casefold()
) -> Dict[Tuple[Optional[int], str], Tag]:
    """{(parent_id, ключ): tag}. key можно подменить (VFS сравнивает по sanitize)."""
    return {(t.parent_id, key(t)): t for t in tags}


def path_of(tag: Tag, by_id: Dict[int, Tag]) -> str:
    """Полный путь тега. Устойчиво к циклам."""
    names: List[str] = []
    seen: set[int] = set()
    cur: Optional[Tag] = tag
    while cur is not None and cur.id not in seen:
        seen.add(cur.id)
        names.append(cur.name)
        cur = by_id.get(cur.parent_id)
    return SEP.join(reversed(names))


def name_map(tags: Iterable[Tag]) -> Dict[str, Tag]:
    """{имя_в_нижнем_регистре: Tag}.

    Тёзки в разных ветках теперь законны, поэтому выбор детерминирован:
    сначала корневой, потом минимальный id. Для уникальных имён — как раньше.
    """
    result: Dict[str, Tag] = {}
    for tag in sorted(tags, key=lambda t: (t.parent_id is not None, t.id or 0)):
        result.setdefault(tag.name.casefold(), tag)
    return result


def path_map(tags: Iterable[Tag]) -> Dict[str, Tag]:
    """{'жанр/детектив': Tag}. Ключи в нижнем регистре."""
    tags = list(tags)
    by_id = {t.id: t for t in tags}
    return {path_of(t, by_id).casefold(): t for t in tags}


def name_to_path(tags: Iterable[Tag]) -> Dict[str, str]:
    """{имя_в_нижнем_регистре: полный путь} — для отображения.

    При тёзках берём самый короткий путь: он стабильнее и читабельнее.
    """
    result: Dict[str, str] = {}
    items = sorted(path_map(tags).items(), key=lambda kv: (kv[0].count(SEP), kv[0]))
    for path, tag in items:
        result.setdefault(tag.name.casefold(), path)
    return result

def resolve(path: str, tags: Iterable[Tag]) -> Optional[Tag]:
    """Тег по полному пути ИЛИ по имени. Регистр не важен. None — если нет.

    Сначала пробуем путь (он точнее), потом имя — имена всё равно уникальны,
    а после перевешивания тег перестаёт быть корневым и его путь меняется.
    """
    tags = list(tags)
    key = normalize(path).casefold()
    return path_map(tags).get(key) or name_map(tags).get(key)

def resolve_all(path: str, tags: Iterable[Tag]) -> List[Tag]:
    """Все теги, подходящие под путь ИЛИ имя. Тёзки — все."""
    tags = list(tags)
    by_id = {t.id: t for t in tags}
    key = normalize(path).casefold()
    hits = [
        t for t in tags
        if t.name.casefold() == key or path_of(t, by_id).casefold() == key
    ]
    return sorted(hits, key=lambda t: (t.parent_id is not None, t.id or 0))


def missing_ancestors(path: str, tags: Iterable[Tag]) -> List[str]:
    """Недостающие звенья, от корня к листу: ['раздел', 'раздел/под']."""
    known = path_map(tags)
    result: List[str] = []
    chunks = parts(path)
    for i in range(1, len(chunks) + 1):
        candidate = SEP.join(chunks[:i])
        if candidate.casefold() not in known:
            result.append(candidate)
    return result


def subtree_names(path: str, tags: Iterable[Tag]) -> List[str]:
    """Имя тега по пути/имени и всех его потомков (для поиска по ветке)."""
    tags = list(tags)
    node = resolve(path, tags)
    if node is None:
        chunks = parts(path)
        return [chunks[-1]] if chunks else []

    ids = {node.id}
    changed = True
    while changed:
        changed = False
        for t in tags:
            if t.parent_id in ids and t.id not in ids:
                ids.add(t.id)
                changed = True
    return [t.name for t in tags if t.id in ids]