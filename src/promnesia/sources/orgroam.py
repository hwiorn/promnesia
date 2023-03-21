#!/usr/bin/env python3

import os
import itertools
from pathlib import Path
from contextlib import nullcontext
from datetime import datetime
from urllib.parse import unquote
from typing import (
    Optional,
    Iterable,
    Union,
    List,
    Tuple,
    NamedTuple,
    Sequence,
    Iterator,
    Iterable,
    Callable,
    Any,
    Dict,
    Set,
    cast,
)
from ..common import (
    Visit,
    get_logger,
    Results,
    Result,
    Loc,
    PathIsh,
    Res,
    file_mtime,
    traverse,
)
from ..config import use_cores
import bibtexparser

import orgparse
from orgparse.date import gene_timestamp_regex, OrgDate
from orgparse.node import OrgNode

from fnmatch import fnmatch
from .filetypes import IGNORE
from .org import get_body_compat, extract_from_file, CREATED_RGX
from .auto import Options, Replacer
from .bib import index as bib_index


class Parsed(NamedTuple):
    dt: Optional[datetime]
    roam_refs: str
    heading: str


bibtex_cite_keys = {}


def _parse_org_roam(n: OrgNode) -> Parsed:
    if n.is_root():
        roam_refs = cast(str, n.get_property("ROAM_REFS", ""))
        roam_date = cast(
            datetime,
            n.get_file_property("DATE")
            or n.get_file_property("CREATED")
            or n.get_property("CREATED", None),
        )
        if roam_date:
            [odt] = OrgDate.list_from_str(roam_date)
            roam_date = odt.start
        return Parsed(dt=roam_date, roam_refs=roam_refs, heading="")

    heading = n.get_heading("raw")
    pp = n.properties
    createds = cast(Optional[str], pp.get("CREATED", None))
    roam_refs = cast(str, pp.get("ROAM_REFS", ""))
    if createds is None:
        # TODO replace with 'match', but need to strip off priority etc first?
        # see _parse_heading in orgparse
        # todo maybe use n.get_timestamps(inactive=True, point=True)? only concern is that it's searching in the body as well?
        m = CREATED_RGX.search(heading)
        if m is not None:
            createds = m.group(0)  # could be None
            # todo a bit hacky..
            heading = heading.replace(createds + " ", "")
    if createds is not None:
        [odt] = OrgDate.list_from_str(createds)
        dt = odt.start
    else:
        dt = None
    return Parsed(dt=dt, roam_refs=roam_refs, heading=heading)


def walk_node(*, node: OrgNode, dt: datetime) -> Iterator[Res[Tuple[Parsed, OrgNode]]]:
    parsed = Parsed(dt=None, roam_refs="", heading="")
    try:
        parsed = _parse_org_roam(node)
    except Exception as e:
        yield e
    else:
        if parsed.dt is None:
            parsed = parsed._replace(dt=dt)
        else:
            dt = parsed.dt
    yield parsed, node

    for c in node.children:
        yield from walk_node(node=c, dt=dt)


def extract_refs_from_roam_file(fname: PathIsh) -> Results:
    """
    Note that org-mode doesn't keep timezone, so we don't really have choice but make it tz-agnostic
    """
    path = Path(fname)
    o = orgparse.load(str(path))
    root = o.root

    fallback_dt = file_mtime(path)

    for wr in walk_node(node=root, dt=fallback_dt):
        if isinstance(wr, Exception):
            yield wr
            continue

        (parsed, node) = wr
        dt = parsed.dt
        assert dt is not None  # shouldn't be because of fallback

        if not parsed.roam_refs or not parsed.roam_refs.strip():
            continue

        cite_key = parsed.roam_refs.strip().replace("cite:", "")
        if cite_key not in bibtex_cite_keys:
            continue

        entry = bibtex_cite_keys[cite_key]

        locator = Loc.file(fname, line=getattr(node, "linenumber", None))
        tagss = "" if len(node.tags) == 0 else f'   :{":".join(sorted(node.tags))}:'
        ctx = parsed.heading + tagss + "\n" + get_body_compat(node)
        for field in "isbn", "issn", "doi", "url":
            if field in entry and entry[field].strip():
                yield Visit(
                    url=unquote(entry[field]) if field == "url" else entry[field],
                    dt=parsed.dt,
                    context=ctx,
                    locator=locator,
                )


def _index_file_aux(path: Path, opts: Options) -> Union[Exception, List[Result]]:
    # just a helper for the concurrent version (the generator isn't picklable)
    try:
        return list(extract_refs_from_roam_file(path)) + list(extract_from_file(path))
    except Exception as e:
        # possible due to unavoidable race conditions
        return e


def _index(path: Path, opts: Options) -> Results:
    logger = get_logger()

    cores = use_cores()
    if cores is None:  # do not use cores
        # todo use ExitStack instead?
        pool = nullcontext()
        mapper = map  # dummy pool
    else:
        workers = None if cores == 0 else cores
        pool = Pool(workers)  # type: ignore
        mapper = pool.map  # type: ignore

    # iterate over resolved paths, to avoid duplicates
    def rit() -> Iterable[Path]:
        it = traverse(path, follow=opts.follow, ignore=IGNORE)
        for p in it:
            if not fnmatch(str(p), "*.org"):
                continue

            yield p

    from more_itertools import unique_everseen

    it = list(unique_everseen(rit()))
    with pool:
        for r in mapper(_index_file_aux, it, itertools.repeat(opts)):
            if isinstance(r, Exception):
                yield r
            else:
                yield from r


def _index_bibtex(path: Path, opts: Options) -> None:
    logger = get_logger()

    # iterate over resolved paths, to avoid duplicates
    def rit() -> Iterable[Path]:
        it = traverse(path, follow=opts.follow, ignore=IGNORE)
        for p in it:
            if not fnmatch(str(p), "*.bib"):
                continue

            p = p.resolve()
            if not os.path.exists(p):
                logger.debug("ignoring %s: broken symlink?", p)
                continue

            yield p

    from more_itertools import unique_everseen

    it = list(unique_everseen(rit()))
    for bib_path in it:
        with open(bib_path) as bib_file:
            parser = bibtexparser.bparser.BibTexParser(common_strings=True)
            bib = bibtexparser.load(bib_file, parser=parser)
            for entry in bib.entries:
                _id = entry["ID"]
                bibtex_cite_keys[_id] = entry
                # _handle_bib_entry(entry)


def index(
    *paths: Union[PathIsh],
    ignored: Union[Sequence[str], str] = (),
    follow: bool = True,
    replacer: Replacer = None,
) -> Results:
    # ignored = (ignored,) if isinstance(ignored, str) else ignored

    # Parse bibtex files first
    for p in paths:
        apath = Path(p).expanduser().resolve().absolute()
        root = apath if apath.is_dir() else None

        opts = Options(ignored=ignored, follow=follow, replacer=replacer, root=root,)
        _index_bibtex(apath, opts=opts)

    # Parse org-roam files next
    for p in paths:
        apath = Path(p).expanduser().resolve().absolute()
        root = apath if apath.is_dir() else None

        opts = Options(ignored=ignored, follow=follow, replacer=replacer, root=root,)
        yield from _index(apath, opts=opts)
