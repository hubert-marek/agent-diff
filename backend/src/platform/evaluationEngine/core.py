from dataclasses import dataclass
from typing import Any
from typing_extensions import Literal
from src.platform.evaluationEngine.compiler import DSLCompiler
from src.platform.evaluationEngine.differ import Differ
from src.platform.evaluationEngine.assertion import AssertionEngine
from src.platform.evaluationEngine.models import DiffResult
from src.platform.isolationEngine.session import SessionManager
from src.platform.db.schema import ChangeJournal
from uuid import uuid4, UUID


@dataclass
class SnapshotResult:
    suffix: str
    schema: str
    environment_id: str


class CoreEvaluationEngine:
    def __init__(self, sessions: SessionManager):
        self.sessions = sessions
        self.compiler = DSLCompiler()

    @staticmethod
    def generate_suffix(prefix: str) -> str:
        return f"{prefix}_{uuid4().hex[:8]}"

    def compile(self, spec: dict[str, Any]) -> dict[str, Any]:
        return self.compiler.compile(spec)

    def take_snapshot(
        self,
        *,
        schema: str,
        environment_id: str,
        prefix: Literal["before", "after"],
        suffix: str | None = None,
    ) -> SnapshotResult:
        suffix = suffix or self.generate_suffix(prefix)
        differ = Differ(
            schema=schema,
            environment_id=environment_id,
            session_manager=self.sessions,
        )
        differ.create_snapshot(suffix)
        return SnapshotResult(
            suffix=suffix, schema=schema, environment_id=environment_id
        )

    def take_before(
        self, *, schema: str, environment_id: str, suffix: str | None = None
    ) -> SnapshotResult:
        return self.take_snapshot(
            schema=schema,
            environment_id=environment_id,
            prefix="before",
            suffix=suffix,
        )

    def take_after(
        self, *, schema: str, environment_id: str, suffix: str | None = None
    ) -> SnapshotResult:
        return self.take_snapshot(
            schema=schema,
            environment_id=environment_id,
            prefix="after",
            suffix=suffix,
        )

    def compute_diff(
        self,
        *,
        schema: str,
        environment_id: str,
        before_suffix: str,
        after_suffix: str,
    ) -> DiffResult:
        differ = Differ(
            schema=schema, environment_id=environment_id, session_manager=self.sessions
        )

        return differ.get_diff(before_suffix, after_suffix)

    def compute_diff_from_journal(
        self,
        *,
        environment_id: str,
        run_id: str,
    ) -> DiffResult:
        env_uuid = UUID(environment_id)
        run_uuid = UUID(run_id)
        with self.sessions.with_meta_session() as session:
            entries = (
                session.query(ChangeJournal)
                .filter(
                    ChangeJournal.environment_id == env_uuid,
                    ChangeJournal.run_id == run_uuid,
                )
                .order_by(ChangeJournal.recorded_at.asc(), ChangeJournal.lsn.asc())
                .all()
            )
            session.query(ChangeJournal).filter(
                ChangeJournal.environment_id == env_uuid,
                ChangeJournal.run_id == run_uuid,
            ).delete(synchronize_session=False)

        inserts: list[dict] = []
        updates: list[dict] = []
        deletes: list[dict] = []

        for entry in entries:
            table = entry.table_name
            if entry.operation == "insert":
                row = dict(entry.after or {})
                row["__table__"] = table
                inserts.append(row)
            elif entry.operation == "delete":
                row = dict(entry.before or {})
                row["__table__"] = table
                deletes.append(row)
            elif entry.operation == "update":
                updates.append(
                    {
                        "__table__": table,
                        "before": entry.before or {},
                        "after": entry.after or {},
                    }
                )

        return DiffResult(inserts=inserts, updates=updates, deletes=deletes)

    def archive(self, *, schema: str, environment_id: str, suffixes: list[str]) -> None:
        differ = Differ(
            schema=schema, environment_id=environment_id, session_manager=self.sessions
        )
        for suffix in suffixes:
            differ.archive_snapshots(suffix)

    def evaluate(
        self,
        *,
        compiled_spec: dict[str, Any],
        diff: DiffResult,
    ) -> dict:
        return AssertionEngine(compiled_spec).evaluate(diff.model_dump())
