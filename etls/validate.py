"""
Schema Validation Module
=========================
Provides JSON-Schema-based validation for raw and transformed records.
Used by both the extract and transform stages to enforce data quality.

**Why validate?**
WebSocket feeds can deliver malformed, partial, or unexpected messages
(heartbeats, error frames, schema changes).  Validating early prevents
corrupt data from propagating through the pipeline and silently poisoning
downstream analytics.
"""

from __future__ import annotations

import logging
from typing import Any

import jsonschema

logger = logging.getLogger("etl.validate")


def validate_record(
    record: dict[str, Any],
    schema: dict[str, Any],
) -> tuple[bool, list[str]]:
    """
    Validate a single record against a JSON Schema.

    Parameters
    ----------
    record : dict
        The data record to validate.
    schema : dict
        A JSON Schema (draft-7 compatible) definition.

    Returns
    -------
    tuple[bool, list[str]]
        ``(True, [])`` when valid, ``(False, [error_messages])`` otherwise.
    """
    validator = jsonschema.Draft7Validator(schema)
    errors = sorted(validator.iter_errors(record), key=lambda e: list(e.path))
    if errors:
        messages = [f"{'.'.join(str(p) for p in e.absolute_path)}: {e.message}" for e in errors]
        return False, messages
    return True, []


def validate_batch(
    records: list[dict[str, Any]],
    schema: dict[str, Any],
    *,
    drop_invalid: bool = True,
) -> tuple[list[dict[str, Any]], int]:
    """
    Validate a batch of records.  Optionally drop invalid ones.

    Returns
    -------
    tuple[list[dict], int]
        (valid_records, count_of_invalid_records)
    """
    valid: list[dict[str, Any]] = []
    invalid_count = 0
    for rec in records:
        ok, errs = validate_record(rec, schema)
        if ok:
            valid.append(rec)
        else:
            invalid_count += 1
            if not drop_invalid:
                valid.append(rec)
            logger.warning("Invalid record dropped: %s", errs)
    logger.info(
        "Validation complete: %d valid, %d invalid out of %d total",
        len(valid), invalid_count, len(records),
    )
    return valid, invalid_count
