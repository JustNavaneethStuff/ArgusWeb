from __future__ import annotations

from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags


def configure_tracing(service_name: str, otlp_endpoint: str) -> None:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    endpoint = otlp_endpoint.replace("http://", "").replace("https://", "")
    if not endpoint.startswith("http"):
        endpoint = otlp_endpoint
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


def get_tracer(name: str):
    return trace.get_tracer(name)


def attach_trace_context(trace_id: str | None) -> object | None:
    """Attach a lightweight trace context from Kafka trace_id header."""
    if not trace_id or len(trace_id) < 16:
        return None
    try:
        padded = trace_id.replace("-", "").ljust(32, "0")[:32]
        trace_id_int = int(padded, 16)
        span_context = SpanContext(
            trace_id=trace_id_int,
            span_id=trace_id_int & 0xFFFFFFFFFFFFFFFF,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
        ctx = trace.set_span_in_context(NonRecordingSpan(span_context))
        from opentelemetry.context import attach

        return attach(ctx)
    except (ValueError, OverflowError):
        return None


def detach_trace_context(token: object | None) -> None:
    if token is None:
        return
    from opentelemetry.context import detach

    detach(token)
