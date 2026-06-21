"""Add index for incremental crawl queries

Revision ID: 002
Revises: 001
Create Date: 2026-06-20
"""

from typing import Sequence, Union

from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index("ix_urls_domain_last_crawled", "urls", ["domain", "last_crawled_at"])


def downgrade() -> None:
    op.drop_index("ix_urls_domain_last_crawled", table_name="urls")
