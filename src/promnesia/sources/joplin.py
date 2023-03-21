#!/usr/bin/env python3

import logging
import textwrap
import re
from os import PathLike
from pathlib import Path
from typing import List, Optional, Iterable

# from .markdown import extract_from_text
from urllib.parse import unquote

# from my.hypothesis import highlights

from .. import config
from ..common import (
    PathIsh,
    Visit,
    get_logger,
    Loc,
    extract_urls,
    from_epoch,
    Results,
    echain,
    join_tags,
)

logger = get_logger()


def joplin_recent_data(suffix: str):
    cache_dir = config.get().cache_dir
    if cache_dir is None:
        return None
    # doesn't need a nontrivial hash function, timestsamp is encoded in name
    return cache_dir / (data.name + "_" + suffix + ".cache")


def index(
    db_path: PathIsh = "~/.config/joplin*/database.sqlite",
    locator_schema="joplin",
    *,
    http_only: bool = None,
) -> Results:
    glob_paths = list(get_files(db_path))
    logger.debug("Expanded path(s): %s", glob_paths)
    assert glob_paths, f"No Joplin-desktop sqlite found: {db_path}"

    _query = notes_query(http_only)

    for db_path in get_files(db_path):
        assert db_path.is_file(), f"Is it a (Joplin-desktop sqlite) file? {db_path}"
        yield from _harvest_db(db_path, _query, locator_schema)


# TODO only upadte new items 마지막 데이터를 저장하고 있다가, DB에서 체크할 때
# 없는 경우 전체 다시 업데이트, 있는 경우, recent 값만 취득
def notes_query(http_only: Optional[bool]) -> str:
    """
    An SQL-query returning 1 row for each notes

    """
    extra_criteria = (
        "AND (title LIKE '%http%' OR body LIKE '%http%' OR url LIKE '%http%')"
        if http_only
        else ""
    )
    return textwrap.dedent(
        f"""
        WITH GTAGS AS (
            SELECT
                NT.note_id,
                GROUP_CONCAT(T.title, ',') as tags
            FROM tags AS T
            LEFT JOIN note_tags AS NT ON T.id = NT.tag_id
            GROUP BY NT.note_id
        )
        SELECT
            id,
            title,
            body,
            created_time,
            updated_time,
            source_url as url,
            markup_language as md,
            T.tags
        FROM notes
        LEFT JOIN GTAGS AS T
            ON T.note_id = id
        WHERE
            body IS NOT NULL
        ORDER BY updated_time
        """
    )


def _handle_fallback_row(row: dict, db_path: PathLike, locator_schema: str) -> Results:
    pass


def _handle_row(row: dict, db_path: PathLike, locator_schema: str) -> Results:
    body: str = row["body"]
    title: str = row["title"]
    src_url: str = row["url"]
    md: int = row["md"]

    dt = from_epoch(row["updated_time"] / 1000)
    # ct = from_epoch(row["created_time"] / 1000)
    note_id: str = row["id"]
    tags: str = row["tags"]
    joined_tags = join_tags(tags.split(",") if tags else "")
    urls: List[str] = extract_urls(body) + extract_urls(title)
    locator = Loc.make(
        title="Joplin", href=f"{locator_schema}://x-callback-url/openNote?id={note_id}",
    )

    for u in urls:
        cparts = [title, body[:200], unquote(u)]
        if joined_tags:
            cparts.append(joined_tags)

        yield Visit(
            url=u, dt=dt, context="\n".join(cparts), locator=locator,
        )

    if src_url:
        # Find markdown highlights
        mark_re = re.compile(r"==(.+?)==" if md == 1 else r"<mark>(.+?)</mark>")
        highlights = mark_re.findall(body)
        if highlights:
            for hl in highlights:
                cparts = [title, hl]
                if joined_tags:
                    cparts.append(joined_tags)

                yield Visit(
                    url=src_url, dt=dt, context="\n".join(cparts), locator=locator,
                )
        else:
            cparts = [title, body[:200], f"clipped: {unquote(src_url)}"]
            if tags:
                cparts.append(join_tags(tags.split(",")))

            yield Visit(
                url=src_url, dt=dt, context="\n".join(cparts), locator=locator,
            )


def dataset_readonly(db: Path):
    import dataset  # type: ignore

    # see https://github.com/pudo/dataset/issues/136#issuecomment-128693122
    import sqlite3

    creator = lambda: sqlite3.connect(f"file:{db}?immutable=1", uri=True)
    return dataset.connect("sqlite:///", engine_kwargs={"creator": creator})


def get_files(path: PathIsh) -> Iterable[Path]:
    """
    Expand homedir(`~`) and return glob paths matched.

    Expansion code copied from https://stackoverflow.com/a/51108375/548792
    """
    path = Path(path).expanduser()
    parts = path.parts[1:] if path.is_absolute() else path.parts
    return Path(path.root).glob(str(Path("").joinpath(*parts)))


def _harvest_db(db_path: PathIsh, msgs_query: str, locator_schema: str):
    is_debug = logger.isEnabledFor(logging.DEBUG)

    # Note: for displaying maybe better not to expand/absolute,
    # but it's safer for debugging resolved.
    db_path = Path(db_path).resolve()

    with dataset_readonly(db_path) as db:
        for row in db.query(msgs_query):
            try:
                yield from _handle_row(row, db_path, locator_schema)
            except Exception as ex:
                # TODO: also insert errors in db
                logger.warning(
                    "Cannot extract row: %s, due to: %s(%s)",
                    row,
                    type(ex).__name__,
                    ex,
                    exc_info=is_debug,
                )
