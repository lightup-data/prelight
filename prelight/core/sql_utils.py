from __future__ import annotations

import re


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
