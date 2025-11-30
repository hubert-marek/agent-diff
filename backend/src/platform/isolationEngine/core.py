import logging
from .session import SessionManager
from .environment import EnvironmentHandler
from .models import EnvironmentResponse
from .models import TemplateCreateResult
from src.platform.db.schema import RunTimeEnvironment
from uuid import uuid4
from datetime import datetime, timedelta

from .pool import PoolManager

logger = logging.getLogger(__name__)


class CoreIsolationEngine:
    def __init__(
        self,
        sessions: SessionManager,
        environment_handler: EnvironmentHandler,
        pool_manager: PoolManager | None = None,
    ):
        self.sessions = sessions
        self.environment_handler = environment_handler
        self.pool_manager = pool_manager or PoolManager(sessions)

    def create_environment(
        self,
        *,
        template_schema: str,
        ttl_seconds: int,
        created_by: str,
        impersonate_user_id: str | None = None,
        impersonate_email: str | None = None,
    ) -> EnvironmentResponse:
        if not self.environment_handler.schema_exists(template_schema):
            logger.error(f"Template schema '{template_schema}' does not exist")
            raise ValueError(f"template schema '{template_schema}' does not exist")

        evn_uuid = uuid4()
        environment_id = evn_uuid.hex
        template_meta = self.environment_handler.get_template_metadata(
            location=template_schema
        )
        table_order = template_meta.table_order if template_meta else None
        template_uuid = str(template_meta.id) if template_meta else None

        import time

        t0 = time.perf_counter()
        pool_entry = self.pool_manager.claim_ready_schema(
            template_schema=template_schema, requested_by=created_by
        )

        reused_schema = pool_entry is not None

        if reused_schema:
            environment_schema = pool_entry.schema_name
            if pool_entry.template_id and template_uuid is None:
                template_uuid = str(pool_entry.template_id)
            logger.info(f"Claimed pooled schema in {time.perf_counter() - t0:.2f}s")
        else:
            environment_schema = f"state_{environment_id}"
            logger.warning(f"Pool miss for {template_schema}, building from scratch...")

            t1 = time.perf_counter()
            self.environment_handler.create_schema(environment_schema)
            logger.info(f"create_schema took {time.perf_counter() - t1:.2f}s")

            t2 = time.perf_counter()
            self.environment_handler.migrate_schema(template_schema, environment_schema)
            logger.info(f"migrate_schema took {time.perf_counter() - t2:.2f}s")

            t3 = time.perf_counter()
            self.environment_handler.seed_data_from_template(
                template_schema, environment_schema, tables_order=table_order
            )
            logger.info(f"seed_data took {time.perf_counter() - t3:.2f}s")

            self.pool_manager.register_entry(
                schema_name=environment_schema,
                template_schema=template_schema,
                template_id=template_meta.id if template_meta else None,
                status="in_use",
            )
            logger.info(f"Total non-pooled build: {time.perf_counter() - t0:.2f}s")

        expires_at = datetime.now() + timedelta(seconds=ttl_seconds)
        self.environment_handler.set_runtime_environment(
            environment_id=environment_id,
            schema=environment_schema,
            expires_at=expires_at,
            last_used_at=datetime.now(),
            created_by=created_by,
            template_id=template_uuid,
            impersonate_user_id=impersonate_user_id,
            impersonate_email=impersonate_email,
        )

        logger.info(
            "Created environment %s from template %s for user %s (pooled=%s)",
            environment_id,
            template_schema,
            created_by,
            reused_schema,
        )

        return EnvironmentResponse(
            environment_id=environment_id,
            schema_name=environment_schema,
            expires_at=expires_at,
            impersonate_user_id=impersonate_user_id,
            impersonate_email=impersonate_email,
        )

    def create_template_from_environment(
        self,
        *,
        environment_id: str,
        service: str,
        name: str,
        description: str | None = None,
        visibility: str = "private",
        owner_id: str | None = None,
        version: str = "v1",
    ) -> TemplateCreateResult:
        rte = self.environment_handler.require_environment(environment_id)
        source_schema = rte.schema

        base = f"{service}_{name}".lower().replace(" ", "_")
        target_schema = base
        if self.environment_handler.schema_exists(target_schema):
            target_schema = f"{base}_{uuid4().hex[:8]}"

        self.environment_handler.clone_schema_from_environment(
            source_schema, target_schema
        )

        table_order = None
        if rte.template_id:
            template_meta = self.environment_handler.get_template_metadata(
                template_id=rte.template_id
            )
            if template_meta and template_meta.table_order:
                table_order = template_meta.table_order

        template_id = self.environment_handler.register_template(
            service=service,
            name=name,
            version=version,
            visibility=visibility,
            description=description,
            owner_id=owner_id,
            kind="schema",
            location=target_schema,
            table_order=table_order,
        )

        return TemplateCreateResult(
            template_id=template_id,
            schema_name=target_schema,
            service=service,
            name=name,
        )

    def get_schema_for_environment(self, environment_id: str) -> str:
        with self.sessions.with_meta_session() as session:
            env = (
                session.query(RunTimeEnvironment)
                .filter(RunTimeEnvironment.id == environment_id)
                .one_or_none()
            )
            if env is None:
                raise ValueError("environment not found")
            return env.schema
