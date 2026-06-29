from __future__ import annotations

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from argus_core.models import CrawlJob, CrawlJobUrl, JobStatus, Page, Url, UrlStatus


async def compute_job_progress(session: AsyncSession, job_id: uuid.UUID) -> dict:
    job = await session.get(CrawlJob, job_id)
    if not job:
        return {}

    status_rows = await session.execute(
        select(CrawlJobUrl.status, func.count())
        .where(CrawlJobUrl.job_id == job_id)
        .group_by(CrawlJobUrl.status)
    )
    urls_by_status = {status: int(count) for status, count in status_rows.all()}

    total = sum(urls_by_status.values())
    terminal = (
        urls_by_status.get("parsed", 0)
        + urls_by_status.get("skipped", 0)
        + urls_by_status.get("failed", 0)
    )
    progress = (terminal / total * 100) if total > 0 else 0.0

    return {
        "id": str(job.id),
        "seed_urls": job.seed_urls,
        "max_depth": job.max_depth,
        "allowed_domains": job.allowed_domains,
        "status": job.status.value,
        "urls_queued": total or job.urls_queued,
        "urls_by_status": urls_by_status,
        "progress_percent": round(min(progress, 100.0), 1),
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }


async def refresh_job_status(session: AsyncSession, job_id: uuid.UUID) -> None:
    """Transition running jobs to completed when all URLs are terminal."""
    job = await session.get(CrawlJob, job_id)
    if not job or job.status != JobStatus.running:
        return

    status_rows = await session.execute(
        select(CrawlJobUrl.status, func.count())
        .where(CrawlJobUrl.job_id == job_id)
        .group_by(CrawlJobUrl.status)
    )
    counts = {status: int(c) for status, c in status_rows.all()}
    total = sum(counts.values())
    if total == 0:
        return

    terminal = counts.get("parsed", 0) + counts.get("skipped", 0) + counts.get("failed", 0)
    if terminal >= total:
        job.status = JobStatus.completed
        await session.commit()


async def list_jobs(session: AsyncSession, limit: int = 20, offset: int = 0) -> tuple[list[dict], int]:
    total = await session.scalar(select(func.count()).select_from(CrawlJob))
    result = await session.execute(
        select(CrawlJob).order_by(CrawlJob.created_at.desc()).limit(limit).offset(offset)
    )
    jobs = result.scalars().all()

    job_ids = [job.id for job in jobs]
    url_counts: dict[uuid.UUID, int] = {}
    if job_ids:
        count_rows = await session.execute(
            select(CrawlJobUrl.job_id, func.count())
            .where(CrawlJobUrl.job_id.in_(job_ids))
            .group_by(CrawlJobUrl.job_id)
        )
        url_counts = {jid: int(c) for jid, c in count_rows.all()}

    items = []
    for job in jobs:
        items.append(
            {
                "id": str(job.id),
                "seed_urls": job.seed_urls,
                "max_depth": job.max_depth,
                "allowed_domains": job.allowed_domains,
                "status": job.status.value,
                "urls_queued": url_counts.get(job.id, job.urls_queued),
                "url_count": url_counts.get(job.id, 0),
                "created_at": job.created_at.isoformat() if job.created_at else None,
            }
        )
    return items, int(total or 0)


async def get_expanded_stats(session: AsyncSession) -> dict:
    jobs_total = await session.scalar(select(func.count()).select_from(CrawlJob))
    pages_indexed = await session.scalar(select(func.count()).select_from(Page))
    failed_urls = await session.scalar(
        select(func.count()).select_from(CrawlJobUrl).where(CrawlJobUrl.status == "failed")
    )

    urls_by_status = {}
    for status in UrlStatus:
        count = await session.scalar(
            select(func.count()).select_from(Url).where(Url.status == status)
        )
        urls_by_status[status.value] = count or 0

    domain_rows = (
        await session.execute(
            select(Url.domain, func.count())
            .group_by(Url.domain)
            .order_by(func.count().desc())
            .limit(20)
        )
    ).all()
    pages_by_domain = {row[0]: row[1] for row in domain_rows}

    from datetime import UTC, datetime, timedelta

    cutoff = datetime.now(UTC) - timedelta(hours=24)
    crawl_rate_24h = await session.scalar(
        select(func.count()).select_from(Url).where(Url.last_crawled_at >= cutoff)
    )

    recent_jobs_result = await session.execute(
        select(CrawlJob).order_by(CrawlJob.created_at.desc()).limit(10)
    )
    recent_job_ids = [j.id for j in recent_jobs_result.scalars()]
    recent_counts: dict[uuid.UUID, int] = {}
    if recent_job_ids:
        rc = await session.execute(
            select(CrawlJobUrl.job_id, func.count())
            .where(CrawlJobUrl.job_id.in_(recent_job_ids))
            .group_by(CrawlJobUrl.job_id)
        )
        recent_counts = {jid: int(c) for jid, c in rc.all()}

    recent_jobs = []
    for job in recent_jobs_result.scalars():
        recent_jobs.append(
            {
                "id": str(job.id),
                "status": job.status.value,
                "urls_queued": recent_counts.get(job.id, job.urls_queued),
                "url_count": recent_counts.get(job.id, 0),
                "created_at": job.created_at.isoformat() if job.created_at else None,
            }
        )

    return {
        "jobs_total": jobs_total or 0,
        "pages_indexed": pages_indexed or 0,
        "urls_by_status": urls_by_status,
        "failed_urls": failed_urls or 0,
        "pages_by_domain": pages_by_domain,
        "crawl_rate_24h": crawl_rate_24h or 0,
        "recent_jobs": recent_jobs,
    }
