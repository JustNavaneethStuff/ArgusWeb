"""Phase 3: scheduled jobs and search settings migration

Revision ID: 003
Revises: 002
Create Date: 2026-06-21
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scheduled_crawl_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("cron_expression", sa.String(64), nullable=False),
        sa.Column("job_config", postgresql.JSONB(), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default="true"),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.add_column("crawl_jobs", sa.Column("urls_queued", sa.Integer(), server_default="0"))


def downgrade() -> None:
    op.drop_column("crawl_jobs", "urls_queued")
    op.drop_table("scheduled_crawl_jobs")
