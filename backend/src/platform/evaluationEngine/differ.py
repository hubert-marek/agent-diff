from sqlalchemy import text
from src.platform.isolationEngine.session import SessionManager
from sqlalchemy import inspect
from datetime import datetime
from src.platform.db.schema import Diff
from typing import Any
from .models import DiffResult


class Differ:
    def __init__(
        self, schema: str, environment_id: str, session_manager: SessionManager
    ):
        self.session_manager = session_manager
        self.schema = schema
        self.environment_id = environment_id
        self.engine = session_manager.base_engine
        self.inspector = inspect(self.engine)
        self.tables = self.inspector.get_table_names(schema=self.schema)
        self.q = self.engine.dialect.identifier_preparer.quote
        self._pk_cache = {}

    def _get_pk_columns(self, table: str) -> list[str]:
        """Get primary key column(s) for a table."""
        if table not in self._pk_cache:
            pk = self.inspector.get_pk_constraint(table, schema=self.schema)
            self._pk_cache[table] = pk["constrained_columns"]
        return self._pk_cache[table]

    def create_snapshot(self, suffix: str) -> None:
        with self.engine.begin() as conn:
            for t in self.tables:
                snapshot_table = f"{t}_snapshot_{suffix}"
                sql = f""" 
                    CREATE TABLE IF NOT EXISTS {self.q(self.schema)}.{self.q(snapshot_table)} AS 
                    SELECT * FROM {self.q(self.schema)}.{self.q(t)}
                """
                conn.execute(text(sql))

    def get_inserts(self, before_suffix: str, after_suffix: str) -> list[dict]:
        inserts: list[dict] = []
        with self.engine.begin() as conn:
            for t in self.tables:
                before_table = f"{t}_snapshot_{before_suffix}"
                after_table = f"{t}_snapshot_{after_suffix}"
                pk_cols = self._get_pk_columns(t)

                if not pk_cols:
                    continue

                join_conditions = " AND ".join(
                    f"a.{self.q(pk)} = b.{self.q(pk)}" for pk in pk_cols
                )
                where_conditions = " AND ".join(
                    f"b.{self.q(pk)} IS NULL" for pk in pk_cols
                )

                q_inserts = f"""
                    SELECT a.*
                    FROM {self.q(self.schema)}.{self.q(after_table)} AS a
                    LEFT JOIN {self.q(self.schema)}.{self.q(before_table)} AS b
                    ON {join_conditions}
                    WHERE {where_conditions}
                """
                rows = conn.execute(text(q_inserts)).mappings().all()
                for r in rows:
                    item = dict(r)
                    item["__table__"] = t
                    inserts.append(item)
        return inserts

    def get_updates(
        self,
        before_suffix: str,
        after_suffix: str,
        exclude_cols: list[str] | None = None,
    ) -> list[dict]:
        updates = []
        with self.engine.begin() as conn:
            for t in self.tables:
                before = f"{t}_snapshot_{before_suffix}"
                after = f"{t}_snapshot_{after_suffix}"
                pk_cols = self._get_pk_columns(t)

                if not pk_cols:
                    continue

                cols = [
                    c["name"] for c in self.inspector.get_columns(t, schema=self.schema)
                ]
                if exclude_cols is not None:
                    compare_cols = [c for c in cols if c not in exclude_cols]
                else:
                    compare_cols = cols

                if not compare_cols:
                    continue

                join_conditions = " AND ".join(
                    f"a.{self.q(pk)} = b.{self.q(pk)}" for pk in pk_cols
                )

                cmp_expr = " OR ".join(
                    f"a.{self.q(c)} IS DISTINCT FROM b.{self.q(c)}"
                    for c in compare_cols
                )

                proj_cols = ", ".join(
                    [f"a.{self.q(c)} AS {self.q(f'after_{c}')}" for c in cols]
                    + [f"b.{self.q(c)} AS {self.q(f'before_{c}')}" for c in cols]
                )
                sql = f"""
                    SELECT {proj_cols}
                    FROM {self.q(self.schema)}.{self.q(after)} AS a
                    JOIN {self.q(self.schema)}.{self.q(before)} AS b
                      ON {join_conditions}
                    WHERE {cmp_expr}
                """
                rows = conn.exec_driver_sql(sql).mappings().all()
                for r in rows:
                    after_map = {c: r.get(f"after_{c}") for c in cols}
                    before_map = {c: r.get(f"before_{c}") for c in cols}
                    updates.append(
                        {
                            "__table__": t,
                            "after": after_map,
                            "before": before_map,
                        }
                    )
        return updates

    def get_deletes(self, before_suffix: str, after_suffix: str) -> list[dict]:
        deletes: list[dict] = []
        with self.engine.begin() as conn:
            for t in self.tables:
                before_table = f"{t}_snapshot_{before_suffix}"
                after_table = f"{t}_snapshot_{after_suffix}"
                pk_cols = self._get_pk_columns(t)

                if not pk_cols:
                    continue

                join_conditions = " AND ".join(
                    f"b.{self.q(pk)} = a.{self.q(pk)}" for pk in pk_cols
                )
                where_conditions = " AND ".join(
                    f"a.{self.q(pk)} IS NULL" for pk in pk_cols
                )

                q_deletes = f"""
                    SELECT b.*
                    FROM {self.q(self.schema)}.{self.q(before_table)} AS b
                    LEFT JOIN {self.q(self.schema)}.{self.q(after_table)} AS a
                    ON {join_conditions}
                    WHERE {where_conditions}
                """
                rows = conn.execute(text(q_deletes)).mappings().all()
                for r in rows:
                    item = dict(r)
                    item["__table__"] = t
                    deletes.append(item)
        return deletes

    def get_diff(self, before_suffix: str, after_suffix: str) -> DiffResult:
        inserts = self.get_inserts(before_suffix, after_suffix)
        updates = self.get_updates(before_suffix, after_suffix)
        deletes = self.get_deletes(before_suffix, after_suffix)
        return DiffResult(inserts=inserts, updates=updates, deletes=deletes)

    def archive_snapshots(self, suffix: str) -> None:
        with self.engine.begin() as conn:
            for t in self.tables:
                snapshot_table = f"{t}_snapshot_{suffix}"
                sql = f"""
                    DROP TABLE IF EXISTS {self.q(self.schema)}.{self.q(snapshot_table)}
                """
                conn.execute(text(sql))

    def store_diff(
        self,
        diff: DiffResult,
        before_suffix: str,
        after_suffix: str,
    ) -> None:
        with self.session_manager.with_meta_session() as session:
            diff_object = Diff(
                environment_id=self.environment_id,
                before_suffix=before_suffix,
                after_suffix=after_suffix,
                diff=diff.model_dump(mode="json"),
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            session.add(diff_object)
