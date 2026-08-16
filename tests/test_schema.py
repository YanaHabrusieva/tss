"""The schema guard (`PRAGMA user_version`).

Not a migration system, and not trying to be one — migrations are out of scope
for the POC. This is here because the failure it prevents is SILENT: every table
is created with `CREATE TABLE IF NOT EXISTS`, so an older database keeps the
constraints it was born with. A file written before `dead_letter` was removed
from the outcome CHECK goes on accepting `outcome='dead_letter'` forever, and
nothing anywhere errors — the first symptom is a report that disagrees with the
code that produced it.

Refusing to open is loud, immediate, and one `rm` from fixed.
"""

from __future__ import annotations

import sqlite3

import pytest

from tss.core.store import SCHEMA_VERSION, SchemaVersionError, Store


def test_a_new_database_is_stamped_with_the_current_version(db_path):
    store = Store(db_path)
    store.init_schema()

    assert store.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    store.close()

    # ...and reopening it is uneventful.
    again = Store(db_path)
    assert again.get_job("nothing") is None
    again.close()


def test_opening_a_database_from_an_older_schema_raises(db_path, store):
    """The dead-letter CHECK change is exactly this case: same tables, different
    constraints, no error until something writes a value the current code thinks
    is impossible."""
    store.conn.execute("PRAGMA user_version = 1")
    store.close()

    stale = Store(db_path)
    with pytest.raises(SchemaVersionError) as raised:
        stale.get_job("job-A")

    message = str(raised.value)
    assert db_path in message, "the message must name the file"
    assert "rm -f" in message, "...and say how to fix it"
    assert "schema version 1" in message
    assert f"expects {SCHEMA_VERSION}" in message


def test_a_database_from_before_the_guard_existed_raises(db_path, store):
    """Tables but no stamp: written by a build that predates this check."""
    store.conn.execute("PRAGMA user_version = 0")
    store.close()

    with pytest.raises(SchemaVersionError):
        Store(db_path).get_job("job-A")


def test_a_newer_database_raises_too(db_path, store):
    """Rolling the code back is as dangerous as rolling it forward."""
    store.conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    store.close()

    with pytest.raises(SchemaVersionError):
        Store(db_path).get_job("job-A")


def test_the_guard_leaves_no_connection_behind(db_path, store):
    """A refused open must not leave a live connection on the thread — the next
    call has to fail the same way, not sail past a cached handle."""
    store.conn.execute("PRAGMA user_version = 1")
    store.close()

    stale = Store(db_path)
    for _ in range(2):
        with pytest.raises(SchemaVersionError):
            stale.get_job("job-A")


def test_init_schema_refuses_to_touch_a_mismatched_database(db_path, store):
    """The service calls init_schema() on startup, so this is where a stale file
    actually gets caught in practice: TSS fails to boot instead of running on a
    schema whose constraints it does not agree with."""
    store.conn.execute("PRAGMA user_version = 1")
    store.close()

    with pytest.raises(SchemaVersionError):
        Store(db_path).init_schema()


def test_an_empty_file_is_treated_as_new(db_path):
    """`sqlite3.connect` creates the file on open; an empty one is not stale."""
    sqlite3.connect(db_path).close()

    store = Store(db_path)
    store.init_schema()

    assert store.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    store.close()
