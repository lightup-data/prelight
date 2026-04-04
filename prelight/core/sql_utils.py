from __future__ import annotations

import re

# Pattern for valid SQL identifiers: alphanumeric, underscores, dots (for schema.table)
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def validate_identifier(name: str, label: str = "identifier") -> str:
    """Validate that a name is a safe SQL identifier (no injection risk).

    Allows alphanumeric characters, underscores, and dots (for schema.table notation).
    Raises ValueError if the name contains unexpected characters.
    """
    name = name.strip()
    if not name:
        raise ValueError(f"❌ {label} must not be empty.")
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(
            f"❌ Invalid {label}: '{name}'. "
            f"Only letters, digits, underscores, and dots are allowed."
        )
    return name


def slug(text: str, max_len: int = 40) -> str:
    """'Apply 10% discount to orders' → 'apply-10-discount-to-orders'"""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:max_len]


def rewrite_to_production(sql: str, schema: str, sandbox_name: str, source_table: str) -> str:
    """Rewrite sandbox table references back to production table names."""
    sql = re.sub(
        rf"\b{re.escape(schema)}\.{re.escape(sandbox_name)}\b",
        f"{schema}.{source_table}",
        sql,
        flags=re.IGNORECASE,
    )
    return re.sub(
        rf"\b{re.escape(sandbox_name)}\b",
        source_table,
        sql,
        flags=re.IGNORECASE,
    )
