from __future__ import annotations

import re


def _smart_column_desc(column_name: str, data_type: str) -> str:
    """Return an inferred description for this column based on its name pattern.
    Falls back to a prompt only when meaning cannot be inferred."""
    name = column_name.lower()

    if name == "id":
        return "Unique identifier for this record."
    if name.endswith("_id"):
        entity = name[:-3].replace("_", " ")
        return f"Reference to the associated {entity}."
    if name == "status" or name.endswith("_status"):
        return "Current status of the record."
    if any(x in name for x in ("amount", "price", "cost", "revenue", "total", "value")):
        return "Monetary value in the record's currency."
    if name in ("created_at", "created_on", "creation_date"):
        return "Timestamp when the record was created."
    if name in ("updated_at", "modified_at", "last_updated"):
        return "Timestamp of the most recent update to this record."
    if name in ("deleted_at", "deleted_on"):
        return "Soft-delete timestamp; NULL when the record is active."
    if name.endswith("_at") or name.endswith("_date") or name.endswith("_time"):
        event = name.replace("_at", "").replace("_date", "").replace("_time", "").replace("_", " ").strip()
        return f"Timestamp recording when {event} occurred."
    if any(x in name for x in ("type", "kind", "category")):
        return "Categorical classification of the record."
    if name.startswith("is_") or name.startswith("has_") or "flag" in name:
        return "Boolean flag."
    if any(x in name for x in ("pct", "percent", "rate", "ratio")):
        return "Ratio or percentage value."
    if name in ("count", "qty", "quantity") or name.endswith("_count") or name.startswith("count_"):
        return "Numeric count."
    if any(x in name for x in ("name", "title", "label", "description", "desc")):
        return "Human-readable label or name."
    if any(x in name for x in ("email", "phone", "address")):
        return f"Contact field: {name.replace('_', ' ')}."
    if name in ("country", "region", "city", "state", "zip", "postal_code"):
        return f"Geographic field: {name.replace('_', ' ')}."

    return f"_What does `{column_name}` represent? What values can it have?_"


def _build_columns_table(schema_columns: list[dict]) -> str:
    lines = [
        "| Column | Type | Description |",
        "|--------|------|-------------|",
    ]
    for col in schema_columns:
        name = col["column_name"]
        dtype = col["data_type"]
        prompt = _smart_column_desc(name, dtype)
        lines.append(f"| {name} | {dtype} | {prompt} |")
    return "\n".join(lines)


def _get_section_body(content: str, section_name: str) -> str | None:
    """Return the body of a ## section (everything after its header line)."""
    pattern = rf"\n## {re.escape(section_name)}\n(.*?)(?=\n## |\Z)"
    match = re.search(pattern, content, re.DOTALL)
    return match.group(1) if match else None


def _replace_section_body(content: str, section_name: str, new_body: str) -> str:
    """Replace the body of a ## section, keeping the header line intact."""
    pattern = rf"(\n## {re.escape(section_name)}\n)(.*?)(?=\n## |\Z)"
    return re.sub(
        pattern,
        lambda m: m.group(1) + new_body,
        content,
        flags=re.DOTALL,
    )


def _documented_column_names(columns_body: str) -> set[str]:
    """Return lowercase column names already present in the markdown table."""
    names = set()
    for line in columns_body.split("\n"):
        line = line.strip()
        if not line.startswith("|") or not line.endswith("|"):
            continue
        parts = [p.strip() for p in line.split("|")[1:-1]]
        if not parts:
            continue
        first = parts[0]
        if first and first.lower() not in ("column", "") and not first.startswith("-"):
            names.add(first.lower())
    return names


def _append_new_columns(columns_body: str, schema_columns: list[dict]) -> str:
    """Add rows for any columns not yet in the table; leave existing rows untouched."""
    documented = _documented_column_names(columns_body)
    new_rows = []
    for col in schema_columns:
        if col["column_name"].lower() not in documented:
            name = col["column_name"]
            dtype = col["data_type"]
            prompt = _smart_column_desc(name, dtype)
            new_rows.append(f"| {name} | {dtype} | {prompt} |")
    if not new_rows:
        return columns_body
    return columns_body.rstrip("\n") + "\n" + "\n".join(new_rows) + "\n"


def build_context_md(
    source_table: str,
    schema: str,
    schema_columns: list[dict],
    description: str,
    iso_ts: str,
    context_notes: str | None = None,
    existing_content: str | None = None,
) -> str:
    """Build or update context/{source_table}.md.

    First time (existing_content is None):
      Generate a full template. Column descriptions are inferred from name patterns.
      If context_notes is provided it becomes the Purpose body; otherwise a prompt is used.

    Second+ time (existing_content provided):
      Preserve everything the human already wrote. Only update last_updated
      and append rows for any new columns.
    """
    if existing_content:
        return _update_existing(
            existing_content=existing_content,
            schema_columns=schema_columns,
            iso_ts=iso_ts,
        )
    return _build_fresh(
        source_table=source_table,
        schema=schema,
        schema_columns=schema_columns,
        description=description,
        iso_ts=iso_ts,
        context_notes=context_notes,
    )


def _build_fresh(
    source_table: str,
    schema: str,
    schema_columns: list[dict],
    description: str,
    iso_ts: str,
    context_notes: str | None = None,
) -> str:
    columns_table = _build_columns_table(schema_columns)
    purpose_body = (
        context_notes
        if context_notes
        else (
            f"> _What is the `{source_table}` table built for overall? What business question does it"
            f' answer? (Migration description: "{description}" — expand on the broader purpose of this'
            f" table below.)_"
        )
    )
    banner = (
        ""
        if context_notes
        else (
            f"\n> ⚠️ Auto-generated from sandbox migration. Fill in the prompts below before merging"
            f" — it takes 2 minutes and saves hours later.\n"
        )
    )
    return (
        f"---\n"
        f"table: {source_table}\n"
        f"schema: {schema}\n"
        f"last_updated: {iso_ts}\n"
        f"---\n"
        f"{banner}"
        f"\n"
        f"# {source_table}\n"
        f"\n"
        f"## Purpose\n"
        f"\n"
        f"{purpose_body}\n"
        f"\n"
        f"## Columns\n"
        f"\n"
        f"{columns_table}\n"
        f"\n"
        f"## Metrics\n"
        f"\n"
        f"> _Define key metrics computable from this table. Example:_\n"
        f"> - **Revenue**: `SUM(amount) WHERE status = 'completed'`\n"
    )


def _update_existing(
    existing_content: str,
    schema_columns: list[dict],
    iso_ts: str,
) -> str:
    content = existing_content

    # Update last_updated in frontmatter
    content = re.sub(
        r"^last_updated:.*$",
        f"last_updated: {iso_ts}",
        content,
        flags=re.MULTILINE,
    )

    # Columns — add rows for new columns; leave existing descriptions untouched
    columns_body = _get_section_body(content, "Columns")
    if columns_body is not None:
        updated_columns = _append_new_columns(columns_body, schema_columns)
        content = _replace_section_body(content, "Columns", updated_columns)

    return content
