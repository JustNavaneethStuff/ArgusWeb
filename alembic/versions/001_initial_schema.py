"""Initial schema

Revision ID: 001
Revises:
Create Date: 2026-06-20
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    job_status = sa.Enum("pending", "running", "completed", "failed", name="jobstatus")
    url_status = sa.Enum("pending", "fetched", "parsed", "failed", name="urlstatus")
    job_status.create(op.get_bind(), checkfirst=True)
    url_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "crawl_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("seed_urls", postgresql.JSONB(), nullable=False),
        sa.Column("max_depth", sa.Integer(), server_default="1"),
        sa.Column("allowed_domains", postgresql.JSONB(), nullable=False),
        sa.Column("status", job_status, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "urls",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("crawl_jobs.id")),
        sa.Column("canonical_url", sa.Text(), nullable=False),
        sa.Column("normalized_url", sa.String(2048), nullable=False, unique=True),
        sa.Column("domain", sa.String(255), nullable=False),
        sa.Column("status", url_status, server_default="pending"),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("depth", sa.Integer(), server_default="0"),
        sa.Column("last_crawled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_urls_domain", "urls", ["domain"])
    op.create_index("ix_urls_normalized_url", "urls", ["normalized_url"])

    op.create_table(
        "html_artifacts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("url_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("urls.id"), unique=True),
        sa.Column("storage_key", sa.String(1024), nullable=False),
        sa.Column("checksum", sa.String(64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("http_status", sa.Integer(), server_default="200"),
        sa.Column("fetched_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "pages",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("url_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("urls.id"), unique=True),
        sa.Column("title", sa.String(1024), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("language", sa.String(16), nullable=True),
        sa.Column("text_snippet", sa.Text(), nullable=True),
        sa.Column("extracted_links", postgresql.JSONB(), server_default="[]"),
        sa.Column("metadata", postgresql.JSONB(), server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.execute(
        "CREATE INDEX ix_pages_title_trgm ON pages USING gin (title gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX ix_pages_text_snippet_trgm ON pages USING gin (text_snippet gin_trgm_ops)"
    )


def downgrade() -> None:
    op.drop_table("pages")
    op.drop_table("html_artifacts")
    op.drop_table("urls")
    op.drop_table("crawl_jobs")
    op.execute("DROP TYPE IF EXISTS urlstatus")
    op.execute("DROP TYPE IF EXISTS jobstatus")
