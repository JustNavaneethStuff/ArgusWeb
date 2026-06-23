from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from argus_core.models import Page, Url


@dataclass
class SearchResult:
    title: str | None
    url: str
    normalized_url: str
    description: str | None
    text_snippet: str | None
    domain: str
    score: float


async def search_pages(
    session: AsyncSession,
    query: str,
    limit: int = 20,
    offset: int = 0,
    similarity_threshold: float = 0.1,
) -> tuple[list[SearchResult], int]:
    await session.execute(
        text("SELECT set_config('pg_trgm.similarity_threshold', :threshold, true)"),
        {"threshold": str(similarity_threshold)},
    )

    count_sql = text("""
        SELECT COUNT(*)
        FROM pages p
        JOIN urls u ON p.url_id = u.id
        WHERE (p.title IS NOT NULL AND p.title % :q)
           OR (p.text_snippet IS NOT NULL AND p.text_snippet % :q)
    """)
    total = (await session.execute(count_sql, {"q": query})).scalar_one()

    search_sql = text("""
        SELECT
            p.title,
            u.canonical_url,
            u.normalized_url,
            p.description,
            p.text_snippet,
            u.domain,
            GREATEST(
                COALESCE(similarity(p.title, :q), 0),
                COALESCE(similarity(p.text_snippet, :q), 0)
            ) AS score
        FROM pages p
        JOIN urls u ON p.url_id = u.id
        WHERE (p.title IS NOT NULL AND p.title % :q)
           OR (p.text_snippet IS NOT NULL AND p.text_snippet % :q)
        ORDER BY score DESC
        LIMIT :limit OFFSET :offset
    """)
    rows = (
        await session.execute(
            search_sql,
            {"q": query, "limit": limit, "offset": offset},
        )
    ).all()

    results = [
        SearchResult(
            title=row.title,
            url=row.canonical_url,
            normalized_url=row.normalized_url,
            description=row.description,
            text_snippet=row.text_snippet,
            domain=row.domain,
            score=float(row.score),
        )
        for row in rows
    ]
    return results, int(total)


async def get_page_by_url_id(session: AsyncSession, url_id) -> dict | None:
    stmt = (
        select(Page, Url)
        .join(Url, Page.url_id == Url.id)
        .where(Page.url_id == url_id)
    )
    row = (await session.execute(stmt)).first()
    if not row:
        return None
    page, url = row
    return {
        "title": page.title,
        "url": url.canonical_url,
        "normalized_url": url.normalized_url,
        "description": page.description,
        "text_snippet": page.text_snippet,
        "domain": url.domain,
        "language": page.language,
        "extracted_links": page.extracted_links,
        "metadata": page.page_metadata,
    }
