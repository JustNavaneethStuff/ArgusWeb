from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from argus_core.models import CrawlJob, JobStatus, Page, Url, UrlStatus


async def compute_job_progress(session: AsyncSession, job_id: uuid.UUID) -> dict:
    job = await session.get(CrawlJob, job_id)
    if not job:
        return {}

    urls_by_status: dict[str, int] = {}
    for status in UrlStatus:
        count = await session.scalar(
            select(func.count()).select_from(Url).where(
                Url.job_id == job_id, Url.status == status
            )
        )
        urls_by_status[status.value] = count or 0

    total_urls = sum(urls_by_status.values())
    parsed = urls_by_status.get("parsed", 0)
    failed = urls_by_status.get("failed", 0)
    done = parsed + failed

    if job.status == JobStatus.running and job.urls_queued > 0 and done >= job.urls_queued:
        job.status = JobStatus.completed
        await session.commit()
    elif job.status == JobStatus.running and total_urls > 0 and done >= total_urls and job.urls_queued == 0:
        job.status = JobStatus.completed
        await session.commit()

    progress = (done / job.urls_queued * 100) if job.urls_queued > 0 else 0.0

    return {
        "id": str(job.id),
        "seed_urls": job.seed_urls,
        "max_depth": job.max_depth,
        "allowed_domains": job.allowed_domains,
        "status": job.status.value,
        "urls_queued": job.urls_queued,
        "urls_by_status": urls_by_status,
        "progress_percent": round(min(progress, 100.0), 1),
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }


async def list_jobs(session: AsyncSession, limit: int = 20, offset: int = 0) -> tuple[list[dict], int]:
    total = await session.scalar(select(func.count()).select_from(CrawlJob))
    result = await session.execute(
        select(CrawlJob).order_by(CrawlJob.created_at.desc()).limit(limit).offset(offset)
    )
    jobs = result.scalars().all()

    items = []
    for job in jobs:
        url_count = await session.scalar(
            select(func.count()).select_from(Url).where(Url.job_id == job.id)
        )
        items.append(
            {
                "id": str(job.id),
                "seed_urls": job.seed_urls,
                "max_depth": job.max_depth,
                "allowed_domains": job.allowed_domains,
                "status": job.status.value,
                "urls_queued": job.urls_queued,
                "url_count": url_count or 0,
                "created_at": job.created_at.isoformat() if job.created_at else None,
            }
        )
    return items, int(total or 0)


async def get_expanded_stats(session: AsyncSession) -> dict:
    jobs_total = await session.scalar(select(func.count()).select_from(CrawlJob))
    pages_indexed = await session.scalar(select(func.count()).select_from(Page))
    failed_urls = await session.scalar(
        select(func.count()).select_from(Url).where(Url.status == UrlStatus.failed)
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

    cutoff = datetime.now(UTC) - timedelta(hours=24)
    crawl_rate_24h = await session.scalar(
        select(func.count()).select_from(Url).where(Url.last_crawled_at >= cutoff)
    )

    recent_jobs_result = await session.execute(
        select(CrawlJob).order_by(CrawlJob.created_at.desc()).limit(10)
    )
    recent_jobs = []
    for job in recent_jobs_result.scalars():
        url_count = await session.scalar(
            select(func.count()).select_from(Url).where(Url.job_id == job.id)
        )
        recent_jobs.append(
            {
                "id": str(job.id),
                "status": job.status.value,
                "urls_queued": job.urls_queued,
                "url_count": url_count or 0,
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
