#!/usr/bin/env python3

import logging

# import textwrap
# import re
from datetime import datetime
import datetime as dt
from os import PathLike
from pathlib import Path
from typing import List, Optional, Iterable

# from .markdown import extract_from_text
from urllib.parse import unquote

import bibtexparser
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


def index(
    bib_path: PathIsh = "~/.config/joplin*/database.sqlite", locator_schema="joplin",
) -> Results:
    glob_paths = list(get_files(bib_path))
    logger.debug("Expanded path(s): %s", glob_paths)
    assert glob_paths, f"No bibtex found: {bib_path}"

    for bib_path in get_files(bib_path):
        assert bib_path.is_file(), f"Is it a bib file? {bib_path}"
        yield from load_bib(bib_path, locator_schema)


# # TODO only upadte new items 마지막 데이터를 저장하고 있다가, DB에서 체크할 때
# # 없는 경우 전체 다시 업데이트, 있는 경우, recent 값만 취득
# def notes_query(http_only: Optional[bool]) -> str:
#     """
#     An SQL-query returning 1 row for each notes

#     """
#     extra_criteria = (
#         "AND (title LIKE '%http%' OR body LIKE '%http%' OR url LIKE '%http%')"
#         if http_only
#         else ""
#     )
#     return textwrap.dedent(
#         f"""
#         WITH GTAGS AS (
#             SELECT
#                 NT.note_id,
#                 GROUP_CONCAT(T.title, ',') as tags
#             FROM tags AS T
#             LEFT JOIN note_tags AS NT ON T.id = NT.tag_id
#             GROUP BY NT.note_id
#         )
#         SELECT
#             id,
#             title,
#             body,
#             created_time,
#             updated_time,
#             source_url as url,
#             markup_language as md,
#             T.tags
#         FROM notes
#         LEFT JOIN GTAGS AS T
#             ON T.note_id = id
#         WHERE
#             body IS NOT NULL
#         ORDER BY updated_time
#         """
#     )


# def _handle_fallback_row(row: dict, db_path: PathLike, locator_schema: str) -> Results:
#     pass


# def _handle_row(row: dict, db_path: PathLike, locator_schema: str) -> Results:
#     body: str = row["body"]
#     title: str = row["title"]
#     src_url: str = row["url"]
#     md: int = row["md"]

#     dt = from_epoch(row["updated_time"] / 1000)
#     # ct = from_epoch(row["created_time"] / 1000)
#     note_id: str = row["id"]
#     tags: str = row["tags"]
#     joined_tags = join_tags(tags.split(",") if tags else "")
#     urls: List[str] = extract_urls(body) + extract_urls(title)
#     locator = Loc.make(
#         title="Joplin", href=f"{locator_schema}://x-callback-url/openNote?id={note_id}",
#     )

#     for u in urls:
#         cparts = [title, body[:200], unquote(u)]
#         if joined_tags:
#             cparts.append(joined_tags)

#         yield Visit(
#             url=u, dt=dt, context="\n".join(cparts), locator=locator,
#         )

#     if src_url:
#         # Find markdown highlights
#         mark_re = re.compile(r"==(.+?)==" if md == 1 else r"<mark>(.+?)</mark>")
#         highlights = mark_re.findall(body)
#         if highlights:
#             for hl in highlights:
#                 cparts = [title, hl]
#                 if joined_tags:
#                     cparts.append(joined_tags)

#                 yield Visit(
#                     url=src_url, dt=dt, context="\n".join(cparts), locator=locator,
#                 )
#         else:
#             cparts = [title, body[:200], f"clipped: {unquote(src_url)}"]
#             if tags:
#                 cparts.append(join_tags(tags.split(",")))

#             yield Visit(
#                 url=src_url, dt=dt, context="\n".join(cparts), locator=locator,
#             )


# def dataset_readonly(db: Path):
#     import dataset  # type: ignore

#     # see https://github.com/pudo/dataset/issues/136#issuecomment-128693122
#     import sqlite3

#     creator = lambda: sqlite3.connect(f"file:{db}?immutable=1", uri=True)
#     return dataset.connect("sqlite:///", engine_kwargs={"creator": creator})


def get_files(path: PathIsh) -> Iterable[Path]:
    """
    Expand homedir(`~`) and return glob paths matched.

    Expansion code copied from https://stackoverflow.com/a/51108375/548792
    """
    path = Path(path).expanduser()
    parts = path.parts[1:] if path.is_absolute() else path.parts
    return Path(path.root).glob(str(Path("").joinpath(*parts)))


# def _harvest_db(db_path: PathIsh, msgs_query: str, locator_schema: str):
#     is_debug = logger.isEnabledFor(logging.DEBUG)

#     # Note: for displaying maybe better not to expand/absolute,
#     # but it's safer for debugging resolved.
#     db_path = Path(db_path).resolve()

#     with dataset_readonly(db_path) as db:
#         for row in db.query(msgs_query):
#             try:
#                 yield from _handle_row(row, db_path, locator_schema)
#             except Exception as ex:
#                 # TODO: also insert errors in db
#                 logger.warning(
#                     "Cannot extract row: %s, due to: %s(%s)",
#                     row,
#                     type(ex).__name__,
#                     ex,
#                     exc_info=is_debug,
#                 )


def load_bib(bib_path: PathIsh, locator_schema: str):
    is_debug = logger.isEnabledFor(logging.DEBUG)

    # Note: for displaying maybe better not to expand/absolute,
    # but it's safer for debugging resolved.
    bib_path = Path(bib_path).resolve()

    with open(bib_path) as bib_file:
        parser = bibtexparser.bparser.BibTexParser(common_strings=True)
        bib = bibtexparser.load(bib_file, parser=parser)

        for entry in bib.entries:
            try:
                yield from _handle_entry(entry, bib_path, locator_schema)
            except Exception as ex:
                # TODO: also insert errors in bib
                logger.warning(
                    "Cannot extract entry: %s, due to: %s(%s)",
                    entry,
                    type(ex).__name__,
                    ex,
                    exc_info=is_debug,
                )


def _handle_entry(entry: dict, bib_path: PathLike, locator_schema: str) -> Results:
    _id = entry["ID"]
    entry_type = entry["ENTRYTYPE"]
    author = ""
    year = ""
    month = ""
    cparts = []
    keyword = ""

    # https://docs.jabref.org/setup/citationkeypatterns#bibentry-fields
    # TODO: urldate = {2022-02-24},
    # TODO: date = {2018-09-01},

    if "author" in entry:
        author = entry["author"]

    if "keyword" in entry:
        keyword = entry["keyword"]

    cparts = [f"{entry_type} {author} / cite:@{_id}"]
    if "title" in entry:
        cparts.append(entry["title"])

    if "abstract" in entry:
        cparts.append(entry["abstract"])

    # TODO parse date
    _dt = datetime.now()
    # dt = from_epoch(row["updated_time"] / 1000)

    locator = None
    if "jabref" == locator_schema:
        # Jabref dosen't support open citation key
        locator = Loc.make(
            title="JabRef", href=f"{locator_schema}://x-callback-url/bibtex?id={_id}",
        )
    if "zotero" == locator_schema:
        locator = Loc.make(title="Zotero", href=f"zotero://select/items/@[{_id}]",)
    else:
        # TODO: open editor
        locator = Loc.make(title="JabRef", href=f"zotero://select/items/@[{_id}]",)

    if keyword:
        cparts.append(join_tags(keyword.split(",")))

    for field in ["url"]:
        if field in entry and entry[field].strip():
            yield Visit(
                url=unquote(entry[field]),
                dt=_dt,
                context="\n".join(cparts),
                locator=locator,
            )

    for field in "isbn", "issn", "doi":
        if field in entry and entry[field].strip():
            yield Visit(
                url=entry[field], dt=_dt, context="\n".join(cparts), locator=locator,
            )

    # yield Lookup(value=_id)
