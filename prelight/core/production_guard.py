from __future__ import annotations

import logging

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


class ProductionWriteBlockedError(Exception):
    pass


def _bare_table_name(node: exp.Table) -> str:
    return node.name.lower()


def _is_production(table_name: str, sandbox_prefix: str, audit_table: str) -> bool:
    bare = table_name.lower().strip("`\"'")
    return (
        not bare.startswith(sandbox_prefix.lower())
        and bare != audit_table.lower()
    )


def _extract_target_table(statement) -> exp.Table | None:
    """Return the write-target Table node for INSERT/UPDATE/DELETE/MERGE/DROP/ALTER/TRUNCATE."""
    target = getattr(statement, "this", None)
    if target is None:
        return None
    if isinstance(target, exp.Table):
        return target
    if isinstance(target, exp.Schema):
        inner = getattr(target, "this", None)
        if isinstance(inner, exp.Table):
            return inner
    return None


def check_sql(sql: str, sandbox_prefix: str, audit_table: str) -> None:
    """
    Parse sql with sqlglot. Walk all write-operation statements and block any
    that target a production table (not sandbox-prefixed, not the audit table).

    Raises ProductionWriteBlockedError on violation.
    Allows SELECT / WITH-SELECT through silently.
    On parse error, logs a warning and allows through.
    """
    try:
        statements = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.WARN)
    except Exception as e:
        raise ProductionWriteBlockedError(
            f"🚫 SQL could not be parsed — blocking execution as a safety precaution. "
            f"Parse error: {e}. Rewrite the SQL so it can be validated, or check for syntax errors."
        )

    for statement in statements:
        if statement is None:
            continue

        operation: str | None = None
        target_table: exp.Table | None = None

        if isinstance(statement, exp.Insert):
            operation = "INSERT"
            target_table = _extract_target_table(statement)

        elif isinstance(statement, exp.Update):
            operation = "UPDATE"
            target_table = _extract_target_table(statement)

        elif isinstance(statement, exp.Delete):
            operation = "DELETE"
            target_table = _extract_target_table(statement)

        elif isinstance(statement, exp.Merge):
            operation = "MERGE"
            target_table = _extract_target_table(statement)

        elif isinstance(statement, exp.Drop):
            operation = "DROP"
            target_table = _extract_target_table(statement)

        elif isinstance(statement, exp.Alter):
            operation = "ALTER"
            target_table = _extract_target_table(statement)

        elif isinstance(statement, exp.TruncateTable):
            operation = "TRUNCATE"
            # TruncateTable may hold multiple tables under expressions
            for tbl in statement.find_all(exp.Table):
                tbl_name = _bare_table_name(tbl)
                if _is_production(tbl_name, sandbox_prefix, audit_table):
                    raise ProductionWriteBlockedError(
                        f"🚫 Production write blocked: [TRUNCATE] on [{tbl_name}] is not allowed. "
                        f"All changes must go through a sandbox table (prefix: {sandbox_prefix}). "
                        "Use create_sandbox first."
                    )
            continue

        elif isinstance(statement, exp.Create):
            if statement.args.get("replace"):
                operation = "CREATE OR REPLACE"
                target_table = _extract_target_table(statement)

        elif isinstance(statement, (exp.Select, exp.With)):
            # Read-only — always allowed
            continue

        if operation and target_table is not None:
            tbl_name = _bare_table_name(target_table)
            if _is_production(tbl_name, sandbox_prefix, audit_table):
                raise ProductionWriteBlockedError(
                    f"🚫 Production write blocked: [{operation}] on [{tbl_name}] is not allowed. "
                    f"All changes must go through a sandbox table (prefix: {sandbox_prefix}). "
                    "Use create_sandbox first."
                )


_DDL_TYPES = (exp.Alter, exp.Drop, exp.Create, exp.TruncateTable)


def check_not_ddl(sql: str) -> None:
    """Raise ProductionWriteBlockedError if the SQL contains DDL statements.

    DDL (ALTER, DROP, CREATE, TRUNCATE) executes immediately and cannot be previewed
    — there are no rows to show and the change fires the moment the statement runs.
    Callers should direct the user to apply_transformation instead.
    """
    try:
        statements = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.WARN)
    except Exception:
        return  # unparseable — let execute_query surface the error naturally

    for statement in statements:
        if statement is None:
            continue
        if isinstance(statement, _DDL_TYPES):
            stmt_type = type(statement).__name__.upper()
            raise ProductionWriteBlockedError(
                f"⚠️  DDL statements ({stmt_type}) execute immediately and cannot be previewed. "
                f"Call apply_transformation directly to apply this change to the sandbox."
            )


def check_select_only(sql: str) -> None:
    """
    Stricter check for query_table: only SELECT (or WITH…SELECT) is allowed.

    Raises ProductionWriteBlockedError for any other statement type.
    On parse error, logs a warning and allows through.
    """
    try:
        statements = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.WARN)
    except Exception as e:
        raise ProductionWriteBlockedError(
            f"🚫 SQL could not be parsed — blocking execution as a safety precaution. "
            f"Parse error: {e}. Only valid SELECT statements are allowed on production tables."
        )

    for statement in statements:
        if statement is None:
            continue

        if isinstance(statement, exp.Select):
            continue

        if isinstance(statement, exp.With):
            inner = getattr(statement, "this", None)
            if isinstance(inner, exp.Select):
                continue

        stmt_type = type(statement).__name__.upper()
        raise ProductionWriteBlockedError(
            f"🚫 Only SELECT queries are allowed on production tables. "
            f"Got: [{stmt_type}]. Use apply_transformation on a sandbox instead."
        )
