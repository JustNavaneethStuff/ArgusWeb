"""004: processed_events idempotency + crawl_job_urls per-job accounting

Revision ID: 004
Revises: 003
Create Date: 2026-06-29
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "processed_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stage", sa.String(64), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("event_id", "stage", name="uq_processed_events_event_stage"),
    )
    op.create_index("ix_processed_events_event_id", "processed_events", ["event_id"])

    op.create_table(
        "crawl_job_urls",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("crawl_jobs.id")),
        sa.Column("normalized_url", sa.String(2048), nullable=False),
        sa.Column("depth", sa.Integer(), server_default="0"),
        sa.Column("status", sa.String(32), server_default="queued"),
        sa.Column("checkpoint", postgresql.JSONB(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("job_id", "normalized_url", name="uq_crawl_job_urls_job_url"),
    )
    op.create_index("ix_crawl_job_urls_job_id", "crawl_job_urls", ["job_id"])
    op.create_index("ix_crawl_job_urls_status", "crawl_job_urls", ["status"])
    op.create_index(
        "ix_crawl_job_urls_job_status", "crawl_job_urls", ["job_id", "status"]
    )
    op.create_index("ix_urls_job_id", "urls", ["job_id"])


def downgrade() -> None:
    op.drop_index("ix_urls_job_id", table_name="urls")
    op.drop_table("crawl_job_urls")
    op.drop_table("processed_events")
