from __future__ import annotations

import argparse
import csv
import os
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

from dotenv import find_dotenv, load_dotenv

from autotag.db.duckdb_conn import duckdb_conn
from autotag.ingest.downloader import _login_with_retry, _request_with_retry
from autotag.ingest.token_cache import TokenCache
from autotag.utils.paths import get_serving_db_path
from autotag.utils.time import default_business_dt

load_dotenv(find_dotenv())

API_QUERY_USERS_WITH_FILE = os.getenv("MOBILE_QUERY_ENDPOINT", "/userManage/queryUsersWithFile")
MAX_API_BATCH_SIZE = 9999
DEFAULT_BATCH_SIZE = int(os.getenv("MOBILE_QUERY_BATCH_SIZE", str(MAX_API_BATCH_SIZE)))


@dataclass
class SyncStats:
    requested_ids: int = 0
    api_rows: int = 0
    upserted_rows: int = 0
    skipped_rows: int = 0


def _normalize_user_id(value: object) -> str:
    text = str(value or "").strip().strip("\"").strip("'").strip()
    if not text or text in {"-", "None", "null", "NULL"}:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _normalize_mobile(value: object) -> str:
    text = str(value or "").strip().strip("\"").strip("'").strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _is_missing_mobile(mobile: str) -> bool:
    return mobile in {"", "-", "--"}


def _is_masked_mobile(mobile: str) -> bool:
    return "*" in mobile


def _is_valid_mobile(mobile: str) -> bool:
    return not _is_missing_mobile(mobile) and not _is_masked_mobile(mobile)


def _effective_batch_size() -> int:
    size = DEFAULT_BATCH_SIZE
    if size <= 0:
        return MAX_API_BATCH_SIZE
    return min(size, MAX_API_BATCH_SIZE)


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def _ensure_mobile_table(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops_secure")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_secure.user_mobile_secure (
          user_id VARCHAR PRIMARY KEY,
          mobile_number VARCHAR,
          nick_name VARCHAR,
          source VARCHAR,
          first_seen_at TIMESTAMP DEFAULT now(),
          updated_at TIMESTAMP DEFAULT now()
        )
        """
    )
    legacy_exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_tables()
        WHERE schema_name = 'ops' AND table_name = 'user_mobile_secure'
        """
    ).fetchone()[0]
    if legacy_exists:
        conn.execute(
            """
            INSERT INTO ops_secure.user_mobile_secure (user_id, mobile_number, nick_name, source, first_seen_at, updated_at)
            SELECT
              user_id,
              mobile_number,
              nick_name,
              source,
              COALESCE(first_seen_at, now()),
              COALESCE(updated_at, now())
            FROM ops.user_mobile_secure
            WHERE user_id IS NOT NULL AND TRIM(user_id) <> ''
            ON CONFLICT(user_id) DO UPDATE
            SET
              mobile_number = EXCLUDED.mobile_number,
              nick_name = CASE
                WHEN EXCLUDED.nick_name IS NULL OR EXCLUDED.nick_name = ''
                THEN ops_secure.user_mobile_secure.nick_name
                ELSE EXCLUDED.nick_name
              END,
              source = EXCLUDED.source,
              updated_at = now()
            """
        )
        conn.execute("DROP TABLE IF EXISTS ops.user_mobile_secure")

    conn.execute(
        """
        CREATE OR REPLACE VIEW ops."用户手机号表" AS
        SELECT
          user_id AS "用户ID",
          mobile_number AS "手机号",
          nick_name AS "昵称",
          source AS "来源",
          first_seen_at AS "首次入库时间",
          updated_at AS "更新时间"
        FROM ops_secure.user_mobile_secure
        """
    )


def find_missing_login_phone_user_ids(
    conn,
    *,
    dt: str,
    mode: str = "daily",
    include_masked: bool = False,
    limit: int = 0,
) -> list[str]:
    pattern_groups = {
        "daily": {"reg": "user_reg_daily%", "login": "user_login_daily%"},
        "realtime": {"reg": "user_reg_realtime%", "login": "user_login_realtime%"},
        "all": {"reg": "user_reg_%", "login": "user_login_%"},
    }.get(mode, {})
    if not pattern_groups:
        return []

    missing_basic = "(ru.\"手机号\" IS NULL OR TRIM(ru.\"手机号\") = '' OR TRIM(ru.\"手机号\") = '-')"
    masked = "POSITION('*' IN COALESCE(ru.\"手机号\", '')) > 0"
    login_condition = f"({missing_basic}{' OR ' + masked if include_masked else ''})"
    reg_condition = f"({missing_basic} OR {masked})"

    limit_clause = ""
    if limit > 0:
        limit_clause = f" LIMIT {int(limit)}"

    sql = f"""
    SELECT DISTINCT TRIM(BOTH '''' FROM ru."ID") AS user_id
    FROM raw.raw_user ru
    JOIN raw.manifest_files mf
      ON mf.dt = ru.dt
     AND mf.source = 'user'
     AND mf.filename = ru.source_file
     AND mf.hash = ru.file_hash
    WHERE ru.dt = ?::DATE
      AND (
        (mf.task_variant LIKE ? AND {reg_condition})
        OR
        (mf.task_variant LIKE ? AND {login_condition})
      )
      AND TRIM(BOTH '''' FROM ru."ID") <> ''
    ORDER BY 1
    {limit_clause}
    """

    try:
        rows = conn.execute(sql, [dt, pattern_groups["reg"], pattern_groups["login"]]).fetchall()
    except Exception as exc:
        msg = str(exc)
        if "raw.manifest_files" in msg or "raw.raw_user" in msg or "task_variant" in msg:
            return []
        raise
    return [_normalize_user_id(r[0]) for r in rows if _normalize_user_id(r[0])]


def _write_rows_to_xlsx(rows: list[list[str]], target: Path) -> None:
    sheet_rows = []
    for i, row in enumerate(rows, start=1):
        cells = []
        for j, value in enumerate(row, start=1):
            col = chr(ord("A") + j - 1)
            txt = escape(str(value or ""))
            cells.append(f'<c r="{col}{i}" t="inlineStr"><is><t>{txt}</t></is></c>')
        sheet_rows.append(f'<row r="{i}">{"".join(cells)}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(sheet_rows)}</sheetData>'
        "</worksheet>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>"
    )
    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>'
        "</workbook>"
    )
    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        "</Relationships>"
    )

    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels_xml)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)


def _build_user_ids_xlsx(user_ids: list[str]) -> Path:
    fd, path = tempfile.mkstemp(prefix="mobile_query_", suffix=".xlsx")
    os.close(fd)
    target = Path(path)
    rows = [[uid] for uid in user_ids]
    _write_rows_to_xlsx(rows, target)
    return target


def _first_sheet_name(namelist: list[str]) -> str | None:
    candidates = sorted(
        name
        for name in namelist
        if name.startswith("xl/worksheets/") and name.endswith(".xml")
    )
    return candidates[0] if candidates else None


def _load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    target = "xl/sharedStrings.xml"
    if target not in zf.namelist():
        return []
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(zf.read(target))
    out: list[str] = []
    for si in root.findall("x:si", ns):
        texts = [t.text or "" for t in si.findall(".//x:t", ns)]
        out.append("".join(texts))
    return out


def _column_index(cell_ref: str) -> int:
    letters = ""
    for ch in cell_ref:
        if ch.isalpha():
            letters += ch
        else:
            break
    if not letters:
        return 0
    value = 0
    for ch in letters.upper():
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value


def _cell_value(cell: ET.Element, ns: dict[str, str], shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        texts = [t.text or "" for t in cell.findall(".//x:t", ns)]
        return "".join(texts)
    value_node = cell.find("x:v", ns)
    if value_node is None:
        return ""
    raw = value_node.text or ""
    if cell_type == "s":
        try:
            idx = int(raw)
            return shared_strings[idx] if 0 <= idx < len(shared_strings) else ""
        except Exception:
            return ""
    return raw


def _read_rows_from_xlsx(path: Path) -> list[list[str]]:
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path, "r") as zf:
        sheet_name = _first_sheet_name(zf.namelist())
        if not sheet_name:
            return []
        shared_strings = _load_shared_strings(zf)
        root = ET.fromstring(zf.read(sheet_name))

    rows: list[list[str]] = []
    for row in root.findall(".//x:sheetData/x:row", ns):
        v1 = ""
        v2 = ""
        for cell in row.findall("x:c", ns):
            idx = _column_index(cell.attrib.get("r", ""))
            val = _cell_value(cell, ns, shared_strings)
            if idx == 1:
                v1 = val
            elif idx == 2:
                v2 = val
        rows.append([v1, v2])
    return rows


def _read_rows_from_text(path: Path) -> list[list[str]]:
    rows: list[list[str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except Exception:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        for row in reader:
            if not row:
                continue
            rows.append(row)
    return rows


def _looks_like_header(first: str, second: str) -> bool:
    label = f"{first} {second}".lower()
    markers = ["uid", "userid", "user_id", "mobile", "phone", "用户", "手机号", "id"]
    if any(m in label for m in markers):
        if first.isdigit() and second.isdigit():
            return False
        return True
    return False


def _read_records_from_file(path: Path) -> tuple[list[tuple[str, str, str, str]], int]:
    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        rows = _read_rows_from_xlsx(path)
    elif suffix in {".csv", ".txt", ".tsv"}:
        rows = _read_rows_from_text(path)
    else:
        return [], 0

    records: list[tuple[str, str, str, str]] = []
    skipped = 0
    for i, row in enumerate(rows):
        if len(row) < 2:
            skipped += 1
            continue

        uid = _normalize_user_id(row[0])
        mobile = _normalize_mobile(row[1])
        nick = _normalize_text(row[2]) if len(row) > 2 else ""

        if i == 0 and _looks_like_header(uid, mobile):
            continue

        if not uid or not _is_valid_mobile(mobile):
            skipped += 1
            continue

        records.append((uid, mobile, nick, f"import_dir:{path.name}"))

    return records, skipped


def _upsert_mobile_records(conn, rows: list[tuple[str, str, str, str]]) -> int:
    if not rows:
        return 0

    dedup: dict[str, tuple[str, str, str]] = {}
    for uid, mobile, nick, source in rows:
        dedup[uid] = (mobile, nick, source)

    payload = [(uid, v[0], v[1], v[2]) for uid, v in dedup.items()]

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE temp_mobile_upsert (
          user_id VARCHAR,
          mobile_number VARCHAR,
          nick_name VARCHAR,
          source VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO temp_mobile_upsert (user_id, mobile_number, nick_name, source) VALUES (?, ?, ?, ?)", payload
    )

    conn.execute(
        """
        UPDATE ops_secure.user_mobile_secure AS t
        SET
          mobile_number = s.mobile_number,
          nick_name = CASE WHEN s.nick_name IS NULL OR s.nick_name = '' THEN t.nick_name ELSE s.nick_name END,
          source = s.source,
          updated_at = now()
        FROM temp_mobile_upsert s
        WHERE t.user_id = s.user_id
        """
    )

    conn.execute(
        """
        INSERT INTO ops_secure.user_mobile_secure (user_id, mobile_number, nick_name, source, first_seen_at, updated_at)
        SELECT
          s.user_id,
          s.mobile_number,
          NULLIF(s.nick_name, ''),
          s.source,
          now(),
          now()
        FROM temp_mobile_upsert s
        WHERE NOT EXISTS (
          SELECT 1 FROM ops_secure.user_mobile_secure t WHERE t.user_id = s.user_id
        )
        """
    )
    return len(payload)


def _login_headers() -> tuple[str, dict[str, str]]:
    base_url = os.getenv("BASE_URL", "").strip()
    username = os.getenv("API_USERNAME", "").strip()
    password = os.getenv("API_PASSWORD", "").strip()
    totp_secret = os.getenv("TOTP_SECRET", "").strip()

    if not base_url:
        raise RuntimeError("missing env: BASE_URL")
    if not username:
        raise RuntimeError("missing env: API_USERNAME")
    if not password:
        raise RuntimeError("missing env: API_PASSWORD")
    if not totp_secret:
        raise RuntimeError("missing env: TOTP_SECRET")

    token_cache = TokenCache()
    token = token_cache.get_or_refresh()
    if not token:
        token = _login_with_retry(
            base_url=base_url,
            username=username,
            password=password,
            totp_secret=totp_secret,
            max_retries=3,
        )
        token_cache.save_token(token, ttl_hours=48)

    return base_url, {"Authorization": f"Bearer {token}"}


def _query_users_with_file(base_url: str, headers: dict[str, str], user_ids: list[str]) -> list[tuple[str, str, str, str]]:
    temp = _build_user_ids_xlsx(user_ids)
    try:
        with open(temp, "rb") as fp:
            resp = _request_with_retry(
                "POST",
                f"{base_url}{API_QUERY_USERS_WITH_FILE}",
                headers=headers,
                files={
                    "file": (
                        "mobile_query_ids.xlsx",
                        fp,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                },
                timeout=120,
            )
        payload = resp.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"queryUsersWithFile failed: code={payload.get('code')}, msg={payload.get('msg')}")

        rows: list[tuple[str, str, str, str]] = []
        for item in payload.get("data", []):
            uid = _normalize_user_id(item.get("id"))
            mobile = _normalize_mobile(item.get("mobile_number"))
            nick = _normalize_text(item.get("nick_name"))
            if not uid or not _is_valid_mobile(mobile):
                continue
            rows.append((uid, mobile, nick, "api:queryUsersWithFile"))
        return rows
    finally:
        temp.unlink(missing_ok=True)


def sync_mobile_for_user_ids(conn, *, user_ids: list[str]) -> SyncStats:
    stats = SyncStats(requested_ids=len(user_ids))
    if not user_ids:
        return stats

    clean_ids = []
    seen: set[str] = set()
    for uid in user_ids:
        norm = _normalize_user_id(uid)
        if norm and norm not in seen:
            seen.add(norm)
            clean_ids.append(norm)

    if not clean_ids:
        return stats

    batch_size = _effective_batch_size()
    base_url, headers = _login_headers()

    for chunk in _chunked(clean_ids, batch_size):
        rows = _query_users_with_file(base_url, headers, chunk)
        stats.api_rows += len(rows)
        stats.upserted_rows += _upsert_mobile_records(conn, rows)
        stats.skipped_rows += max(0, len(chunk) - len(rows))
        print(
            f"[mobile_sync] batch requested={len(chunk)} api_rows={len(rows)} upserted={stats.upserted_rows}",
            flush=True,
        )

    return stats


def sync_missing_login_phones(
    conn,
    *,
    dt: str,
    mode: str = "daily",
    include_masked: bool = False,
    limit: int = 0,
    dry_run: bool = False,
) -> SyncStats:
    _ensure_mobile_table(conn)
    ids = find_missing_login_phone_user_ids(
        conn,
        dt=dt,
        mode=mode,
        include_masked=include_masked,
        limit=limit,
    )
    print(
        f"[mobile_sync] dt={dt} mode={mode} missing_ids={len(ids)} include_masked={int(include_masked)}",
        flush=True,
    )
    if dry_run:
        return SyncStats(requested_ids=len(ids))
    return sync_mobile_for_user_ids(conn, user_ids=ids)


def import_mobile_from_dir(conn, *, directory: Path, recursive: bool = True) -> SyncStats:
    _ensure_mobile_table(conn)

    if not directory.exists() or not directory.is_dir():
        raise RuntimeError(f"directory not found: {directory}")

    files: list[Path]
    if recursive:
        files = sorted([p for p in directory.rglob("*") if p.is_file()])
    else:
        files = sorted([p for p in directory.glob("*") if p.is_file()])

    supported = {".csv", ".txt", ".tsv", ".xlsx"}
    target_files = [p for p in files if p.suffix.lower() in supported]
    if not target_files:
        raise RuntimeError(f"no supported files in directory: {directory}")

    stats = SyncStats()
    for file in target_files:
        rows, skipped = _read_records_from_file(file)
        upserted = _upsert_mobile_records(conn, rows)
        stats.requested_ids += len(rows)
        stats.api_rows += len(rows)
        stats.upserted_rows += upserted
        stats.skipped_rows += skipped
        print(
            f"[mobile_sync] import file={file} valid_rows={len(rows)} skipped_rows={skipped} upserted={upserted}",
            flush=True,
        )

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sensitive mobile number sync/import")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sync_parser = sub.add_parser("sync-missing", help="sync missing phones from user_reg_* and user_login_*")
    sync_parser.add_argument("--dt", default=default_business_dt())
    sync_parser.add_argument("--mode", choices=["daily", "realtime", "all"], default="daily")
    sync_parser.add_argument("--include-masked", action="store_true")
    sync_parser.add_argument("--limit", type=int, default=0)
    sync_parser.add_argument("--dry-run", action="store_true")

    import_parser = sub.add_parser("import-dir", help="import user_id/mobile files from a directory")
    import_parser.add_argument("--dir", required=True, dest="directory")
    import_parser.add_argument("--no-recursive", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with duckdb_conn(get_serving_db_path()) as conn:
        if args.cmd == "sync-missing":
            include_masked = args.include_masked or os.getenv("MOBILE_SYNC_INCLUDE_MASKED", "0") == "1"
            stats = sync_missing_login_phones(
                conn,
                dt=args.dt,
                mode=args.mode,
                include_masked=include_masked,
                limit=args.limit,
                dry_run=args.dry_run,
            )
            print(
                (
                    f"[mobile_sync] done cmd=sync-missing requested_ids={stats.requested_ids} "
                    f"api_rows={stats.api_rows} upserted_rows={stats.upserted_rows} skipped_rows={stats.skipped_rows}"
                ),
                flush=True,
            )
            return

        if args.cmd == "import-dir":
            stats = import_mobile_from_dir(
                conn,
                directory=Path(args.directory).resolve(),
                recursive=not args.no_recursive,
            )
            print(
                (
                    f"[mobile_sync] done cmd=import-dir loaded_rows={stats.requested_ids} "
                    f"upserted_rows={stats.upserted_rows} skipped_rows={stats.skipped_rows}"
                ),
                flush=True,
            )
            return

    raise RuntimeError(f"unsupported cmd: {args.cmd}")


if __name__ == "__main__":
    main()
