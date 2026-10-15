"""Tag paths: "genre/mystery".

In the DB tags are flat (id, name, parent_id); parent_id defines the hierarchy.
This module converts it to a path and back — nothing else does that.

Why paths: the CLI and the virtual FS show the hierarchy as folders,
and "genre/mystery" is less ambiguous than "move mystery under genre".

resolve accepts both a full path and a bare name: after `parent b a` the path
of tag `b` becomes `a/b`, but it still has to be found by name. Names may repeat
across branches, so name lookup is deterministic (see name_map).
"""

from typing import Callable, Dict, Iterable, List, Optional, Tuple

from meret.core.models import Tag

SEP = "/"


def normalize(path: str) -> str:
    """' /genre//mystery/ ' → 'genre/mystery'."""
    return SEP.join(p.strip() for p in path.split(SEP) if p.strip())


def parts(path: str) -> List[str]:
    norm = normalize(path)
    return norm.split(SEP) if norm else []


def index_by_parent(
    tags: Iterable[Tag], key: Callable[[Tag], str] = lambda t: t.name.casefold()
) -> Dict[Tuple[Optional[int], str], Tag]:
    """{(parent_id, key): tag}. The key can be overridden (VFS compares by sanitize)."""
    return {(t.parent_id, key(t)): t for t in tags}


def path_of(tag: Tag, by_id: Dict[int, Tag]) -> str:
    """Full tag path. Robust to cycles."""
    names: List[str] = []
    seen: set[int] = set()
    cur: Optional[Tag] = tag
    while cur is not None and cur.id not in seen:
        seen.add(cur.id)
        names.append(cur.name)
        cur = by_id.get(cur.parent_id)
    return SEP.join(reversed(names))


def name_map(tags: Iterable[Tag]) -> Dict[str, Tag]:
    """{lowercase_name: Tag}.

    Namesakes in different branches are now allowed, so the choice is deterministic:
    root first, then the lowest id. Unique names behave as before.
    """
    result: Dict[str, Tag] = {}
    for tag in sorted(tags, key=lambda t: (t.parent_id is not None, t.id or 0)):
        result.setdefault(tag.name.casefold(), tag)
    return result


def path_map(tags: Iterable[Tag]) -> Dict[str, Tag]:
    """{'genre/mystery': Tag}. Keys are lowercase."""
    tags = list(tags)
    by_id = {t.id: t for t in tags}
    return {path_of(t, by_id).casefold(): t for t in tags}


def name_to_path(tags: Iterable[Tag]) -> Dict[str, str]:
    """{lowercase_name: full path} — for display.

    With namesakes we take the shortest path: it's more stable and readable.
    """
    result: Dict[str, str] = {}
    items = sorted(path_map(tags).items(), key=lambda kv: (kv[0].count(SEP), kv[0]))
    for path, tag in items:
        result.setdefault(tag.name.casefold(), path)
    return result

def resolve(path: str, tags: Iterable[Tag]) -> Optional[Tag]:
    """Tag by full path OR by name. Case-insensitive. None if not found.

    Try the path first (it's more precise), then the name: after reparenting, a tag
    stops being a root and its path changes. Namesakes resolve as in name_map.
    """
    tags = list(tags)
    key = normalize(path).casefold()
    return path_map(tags).get(key) or name_map(tags).get(key)

def resolve_all(path: str, tags: Iterable[Tag]) -> List[Tag]:
    """All tags matching the path OR the name. All namesakes included."""
    tags = list(tags)
    by_id = {t.id: t for t in tags}
    key = normalize(path).casefold()
    hits = [
        t for t in tags
        if t.name.casefold() == key or path_of(t, by_id).casefold() == key
    ]
    return sorted(hits, key=lambda t: (t.parent_id is not None, t.id or 0))


def missing_ancestors(path: str, tags: Iterable[Tag]) -> List[str]:
    """Missing segments, from root to leaf: ['section', 'section/sub']."""
    known = path_map(tags)
    result: List[str] = []
    chunks = parts(path)
    for i in range(1, len(chunks) + 1):
        candidate = SEP.join(chunks[:i])
        if candidate.casefold() not in known:
            result.append(candidate)
    return result

def subtree_ids(path: str, tags: Iterable[Tag]) -> List[int]:
    """Ids of the tag (by path/name) and all of its descendants."""
    tags = list(tags)
    node = resolve(path, tags)
    if node is None:
        return []

    ids = {node.id}
    changed = True
    while changed:
        changed = False
        for t in tags:
            if t.parent_id in ids and t.id not in ids:
                ids.add(t.id)
                changed = True
    return sorted(ids)

def subtree_names(path: str, tags: Iterable[Tag]) -> List[str]:
    """Names of the tag (by path/name) and all its descendants (for subtree search)."""
    tags = list(tags)
    ids = subtree_ids(path=path, tags=tags)
    return [t.name for t in tags if t.id in ids]