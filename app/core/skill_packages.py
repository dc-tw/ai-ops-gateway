"""Bounded read-only packages. Never extract files, install dependencies or run code."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
import stat
import zipfile
from pathlib import PurePosixPath
from urllib.parse import urlsplit

import httpx
import yaml

from app.core.skill_store import SkillContent, content_fields, resource_path

UPLOAD_LIMIT = 256 * 1024
GITHUB_LIMIT = 2 * 1024 * 1024
TEXT_LIMIT = 64 * 1024
PACKAGE_ERROR = "Invalid or unsafe read-only Skill package"
REPOSITORY = re.compile(r"^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,99}/[A-Za-z0-9_-][A-Za-z0-9_.-]{0,99}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")


class StrictLoader(yaml.SafeLoader):
    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ValueError(PACKAGE_ERROR)
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str) or key in result:
                raise ValueError(PACKAGE_ERROR)
            result[key] = self.construct_object(value_node, deep=deep)
        return result


def subdirectory(value):
    if value:
        resource_path(value + "/resource.md")
    return value


def _path(value):
    if (
        not value
        or len(value) > 512
        or "\\" in value
        or PurePosixPath(value).is_absolute()
        or any(part in {"", ".", ".."} for part in value.rstrip("/").split("/"))
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError(PACKAGE_ERROR)
    value.encode("utf-8")
    return value


def _manifest(text):
    text = text.replace("\r\n", "\n")
    if not text.startswith("---\n") or "\n---\n" not in text[4:]:
        raise ValueError(PACKAGE_ERROR)
    front, instructions = text[4:].split("\n---\n", 1)
    # Bound nesting too, before the YAML constructor can recurse deeply.
    depth = 0
    for token in yaml.scan(front, Loader=StrictLoader):
        if isinstance(
            token,
            (
                yaml.tokens.BlockMappingStartToken,
                yaml.tokens.BlockSequenceStartToken,
                yaml.tokens.FlowMappingStartToken,
                yaml.tokens.FlowSequenceStartToken,
            ),
        ):
            depth += 1
            if depth > 8:
                raise ValueError(PACKAGE_ERROR)
        elif isinstance(
            token,
            (
                yaml.tokens.BlockEndToken,
                yaml.tokens.FlowMappingEndToken,
                yaml.tokens.FlowSequenceEndToken,
            ),
        ):
            depth -= 1
    data = yaml.load(front, Loader=StrictLoader)
    allowed = {
        "name",
        "description",
        "display_name",
        "required_tools",
        "agent_roles",
        "tags",
        "execution_modes",
        "license",
        "compatibility",
        "metadata",
        "allowed-tools",
    }
    if not isinstance(data, dict) or set(data) - allowed:
        raise ValueError(PACKAGE_ERROR)
    descriptive = {
        key: data.pop(key)
        for key in ("license", "compatibility", "metadata", "allowed-tools")
        if key in data
    }
    for key, value in descriptive.items():
        if key == "metadata":
            if (
                not isinstance(value, dict)
                or len(value) > 16
                or any(not isinstance(k, str) or not isinstance(v, str) for k, v in value.items())
            ):
                raise ValueError(PACKAGE_ERROR)
        elif not isinstance(value, str) or len(value) > 1024:
            raise ValueError(PACKAGE_ERROR)
    return {
        **data,
        "schema_version": 2,
        "display_name": data.get("display_name", data.get("name")),
        "instructions": instructions.strip(),
        "_package_metadata": descriptive,
    }


def parse_package(raw, *, source="upload", repository=None, commit=None, directory=""):
    """Read selected ZIP members into bounded memory; every other member stays opaque."""
    try:
        if source not in {"upload", "github"} or not isinstance(raw, bytes):
            raise ValueError(PACKAGE_ERROR)
        if not raw or len(raw) > (UPLOAD_LIMIT if source == "upload" else GITHUB_LIMIT):
            raise ValueError(PACKAGE_ERROR)
        directory = subdirectory(directory)
        if source == "github" and (
            not REPOSITORY.fullmatch(repository or "") or not COMMIT.fullmatch(commit or "")
        ):
            raise ValueError(PACKAGE_ERROR)
        files = {}
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            members = archive.infolist()
            if not members or len(members) > 2048:
                raise ValueError(PACKAGE_ERROR)
            seen, roots, total = set(), set(), 0
            for info in members:
                if info.orig_filename != info.filename:
                    raise ValueError(PACKAGE_ERROR)  # ZIP readers can truncate NUL names.
                name = _path(info.filename)
                if name in seen:
                    raise ValueError(PACKAGE_ERROR)
                seen.add(name)
                roots.add(name.split("/", 1)[0])
                mode = info.external_attr >> 16
                if (
                    stat.S_ISLNK(mode)
                    or (stat.S_IFMT(mode) not in {0, stat.S_IFREG, stat.S_IFDIR})
                    or info.flag_bits & 1
                    or info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
                ):
                    raise ValueError(PACKAGE_ERROR)
            if source == "github" and len(roots) != 1:
                raise ValueError(PACKAGE_ERROR)
            prefix = (next(iter(roots)) + "/") if source == "github" else ""
            prefix += directory + "/" if directory else ""
            for info in members:
                if info.is_dir() or not info.filename.startswith(prefix):
                    continue
                name = info.filename[len(prefix) :]
                if name != "SKILL.md":
                    resource_path(name)
                if (
                    len(files) >= 9
                    or info.file_size > 16_384
                    or info.file_size < 0
                    or info.file_size > max(1, info.compress_size) * 100
                ):
                    raise ValueError(PACKAGE_ERROR)
                with archive.open(info) as member:
                    data = member.read(min(TEXT_LIMIT - total, 16_384) + 1)
                total += len(data)
                if total > TEXT_LIMIT or len(data) > 16_384 or len(data) != info.file_size:
                    raise ValueError(PACKAGE_ERROR)
                files[name] = data.decode("utf-8")
        if "SKILL.md" not in files:
            raise ValueError(PACKAGE_ERROR)
        body = _manifest(files.pop("SKILL.md"))
        descriptive = body.pop("_package_metadata")
        if descriptive:
            if "package-metadata.json" in files:
                raise ValueError(PACKAGE_ERROR)
            files["package-metadata.json"] = json.dumps(
                descriptive, sort_keys=True, ensure_ascii=False
            )
        body["resources"] = [
            {"path": path, "content": text} for path, text in sorted(files.items())
        ]
        body["provenance"] = {
            "source": source,
            "repository": repository,
            "commit": commit,
            "subdirectory": directory,
            "artifact_sha256": hashlib.sha256(raw).hexdigest(),
        }
        return content_fields(SkillContent.model_validate(body).model_dump())
    except Exception as exc:
        # Never echo paths, YAML constructors, source text, URLs or parse internals.
        raise ValueError(PACKAGE_ERROR) from exc


async def fetch_github(repository, commit, *, client=None):
    async with asyncio.timeout(30):
        return await _fetch_github(repository, commit, client=client)


async def _fetch_github(repository, commit, *, client=None):
    if not REPOSITORY.fullmatch(repository) or not COMMIT.fullmatch(commit):
        raise ValueError("GitHub imports require a repository and full immutable commit")
    owned = client is None
    client = client or httpx.AsyncClient(timeout=20, follow_redirects=False, trust_env=False)
    headers = {
        "Accept": "application/vnd.github+json",
        "Accept-Encoding": "identity",
        "User-Agent": "ai-ops-gateway-readonly-import",
    }
    try:
        # Verify the Git object itself: a tag can also be named with 40 hex
        # characters. Syntax validation alone must not turn a floating ref into
        # supposedly immutable provenance.
        async with client.stream(
            "GET",
            f"https://api.github.com/repos/{repository}/git/commits/{commit}",
            follow_redirects=False,
            headers=headers,
        ) as response:
            metadata = await _read_download(response, 65_536)
            if json.loads(metadata).get("sha") != commit:
                raise ValueError("GitHub commit identity mismatch")
        url = f"https://api.github.com/repos/{repository}/zipball/{commit}"
        for hop in range(2):
            async with client.stream(
                "GET",
                url,
                follow_redirects=False,
                headers=headers,
            ) as response:
                if response.status_code == 302 and hop == 0:
                    location = response.headers.get("location", "")
                    parsed = urlsplit(location)
                    if (
                        parsed.scheme != "https"
                        or parsed.netloc != "codeload.github.com"
                        or parsed.query
                        or parsed.fragment
                        or parsed.path
                        not in {f"/{repository}/legacy.zip/{commit}", f"/{repository}/zip/{commit}"}
                    ):
                        raise ValueError("Unsafe GitHub archive redirect")
                    url = location
                    continue
                return await _read_download(response, GITHUB_LIMIT)
        raise ValueError("GitHub archive unavailable")
    finally:
        if owned:
            await client.aclose()


async def _read_download(response, limit):
    if response.status_code != 200:
        raise ValueError("GitHub source unavailable")
    if response.headers.get("content-encoding", "identity") != "identity":
        raise ValueError("Encoded GitHub source is not supported")
    data = bytearray()
    async for chunk in response.aiter_raw(chunk_size=16_384):
        data.extend(chunk)
        if len(data) > limit:
            raise ValueError("GitHub source exceeds import capacity")
    return bytes(data)
