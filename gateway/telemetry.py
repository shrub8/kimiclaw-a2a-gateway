"""OpenTelemetry: traces, metrics and logs to rotating files under GATEWAY_TELEMETRY_DIR (default ./telemetry).
OTLP export is added only when OTEL_EXPORTER_OTLP_ENDPOINT is set (standard OTEL_* env vars apply).
Telemetry carries IDs, method, status and timing only, never message or reply text."""
import json
import logging
import logging.handlers
import os
import time

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricExporter, MetricExportResult, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult

TELEMETRY_DIR = os.environ.get("GATEWAY_TELEMETRY_DIR", "telemetry")
OTLP_ENDPOINT = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
MAX_BYTES = 10 * 1024 * 1024
BACKUPS = 5

tracer = trace.get_tracer("kimiclaw")
meter = metrics.get_meter("kimiclaw")
requests_total = meter.create_counter("gateway.requests", description="HTTP requests")
request_ms = meter.create_histogram("gateway.request.duration", unit="ms")
agent_ms = meter.create_histogram("gateway.agent.duration", unit="ms")
notify_failures = meter.create_counter("gateway.notify.failures")
breaker_opens = meter.create_counter("gateway.notify.breaker_opens")

EVENTS = logging.getLogger("kimiclaw.telemetry")  # structured events only; not propagated to stdout
EVENTS.propagate = False
EVENTS.setLevel(logging.INFO)


def _rotating(name: str) -> logging.Logger:
    lg = logging.getLogger(f"kimiclaw.telemetry.file.{name}")
    lg.propagate = False
    lg.setLevel(logging.INFO)
    h = logging.handlers.RotatingFileHandler(os.path.join(TELEMETRY_DIR, f"{name}.jsonl"),
                                             maxBytes=MAX_BYTES, backupCount=BACKUPS)
    h.setFormatter(logging.Formatter("%(message)s"))
    lg.addHandler(h)
    return lg


class _FileSpanExporter(SpanExporter):
    def __init__(self):
        self.out = _rotating("traces")

    def export(self, spans):
        for s in spans:
            self.out.info(s.to_json(indent=None))
        return SpanExportResult.SUCCESS


class _FileMetricExporter(MetricExporter):
    def __init__(self):
        super().__init__()
        self.out = _rotating("metrics")

    def export(self, metrics_data, timeout_millis=10_000, **kw):
        self.out.info(metrics_data.to_json(indent=None))
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis=10_000):
        return True

    def shutdown(self, timeout_millis=30_000, **kw):
        pass


class _JsonFormatter(logging.Formatter):
    def format(self, record):
        ctx = trace.get_current_span().get_span_context()
        doc = {"ts": self.formatTime(record), "level": record.levelname, "event": record.getMessage()}
        if ctx.is_valid:
            doc["trace_id"] = format(ctx.trace_id, "032x")
            doc["span_id"] = format(ctx.span_id, "016x")
        doc.update(getattr(record, "fields", {}))
        return json.dumps(doc)


def setup():
    os.makedirs(TELEMETRY_DIR, exist_ok=True)
    resource = Resource.create({"service.name": os.environ.get("OTEL_SERVICE_NAME", "kimiclaw-a2a-gateway")})
    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(_FileSpanExporter()))
    readers = [PeriodicExportingMetricReader(_FileMetricExporter(), export_interval_millis=60_000)]
    logs_handler = logging.handlers.RotatingFileHandler(os.path.join(TELEMETRY_DIR, "logs.jsonl"),
                                                        maxBytes=MAX_BYTES, backupCount=BACKUPS)
    logs_handler.setFormatter(_JsonFormatter())
    EVENTS.addHandler(logs_handler)
    if OTLP_ENDPOINT:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        readers.append(PeriodicExportingMetricReader(OTLPMetricExporter()))
        lp = LoggerProvider(resource=resource)
        lp.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
        EVENTS.addHandler(LoggingHandler(logger_provider=lp))
    trace.set_tracer_provider(tp)
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=readers))


def event(name: str, **fields):
    EVENTS.info(name, extra={"fields": fields})


async def telemetry_middleware(request, call_next):
    started = time.perf_counter()
    with tracer.start_as_current_span("rpc", attributes={
            "http.request.method": request.method, "url.path": request.url.path}) as span:
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            ms = (time.perf_counter() - started) * 1000
            span.set_attribute("http.response.status_code", status)
            attrs = {"http.request.method": request.method, "http.response.status_code": status}
            requests_total.add(1, attrs)
            request_ms.record(ms, attrs)
            event("http_request", method=request.method, path=request.url.path, status=status,
                  duration_ms=round(ms, 1), **getattr(request.state, "ids", {}))


def shutdown():
    for provider in (trace.get_tracer_provider(), metrics.get_meter_provider()):
        if hasattr(provider, "shutdown"):
            provider.shutdown()
