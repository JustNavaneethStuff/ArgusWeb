from prometheus_client import Counter, Histogram

urls_crawled_total = Counter(
    "argus_urls_crawled_total",
    "Total URLs crawled",
    ["status"],
)

crawl_duration_seconds = Histogram(
    "argus_crawl_duration_seconds",
    "Time spent fetching a URL",
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

parser_errors_total = Counter(
    "argus_parser_errors_total",
    "Total parser errors",
    ["error_type"],
)

kafka_messages_total = Counter(
    "argus_kafka_messages_total",
    "Total Kafka messages processed",
    ["topic", "status"],
)

rate_limit_waits_total = Counter(
    "argus_rate_limit_waits_total",
    "Total rate limit wait cycles",
    ["domain"],
)

urls_unchanged_total = Counter(
    "argus_urls_unchanged_total",
    "URLs skipped because content hash unchanged",
)

retry_attempts_total = Counter(
    "argus_retry_attempts_total",
    "Total retry attempts processed",
    ["stage", "result"],
)

dlq_replays_total = Counter(
    "argus_dlq_replays_total",
    "Total DLQ replay attempts",
    ["result"],
)

urls_failed_total = Counter(
    "argus_urls_failed_total",
    "Total URL failures by stage",
    ["stage"],
)
