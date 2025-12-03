"""
OpenTelemetry configuration for distributed tracing and monitoring.

This module sets up OpenTelemetry instrumentation for the NYC 311 database pipeline,
enabling traces to be sent to SigNoz for observability.
"""

import os
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.instrumentation.pymysql import PyMySQLInstrumentor
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor


def setup_telemetry(service_name: str = "nyc311-pipeline", enabled: bool = True) -> trace.Tracer:
    """
    Initialize OpenTelemetry tracing with OTLP exporter.

    Args:
        service_name: Name of the service for trace identification
        enabled: Whether to enable telemetry (set False to disable)

    Returns:
        Configured tracer instance

    Environment Variables:
        OTEL_EXPORTER_OTLP_ENDPOINT: SigNoz collector endpoint (default: http://localhost:4317)
        OTEL_SERVICE_NAME: Override service name
        ENABLE_TELEMETRY: Set to 'false' to disable telemetry
    """

    # Check if telemetry is disabled
    if not enabled or os.getenv("ENABLE_TELEMETRY", "true").lower() == "false":
        print("[ℹ️] Telemetry disabled")
        trace.set_tracer_provider(TracerProvider())
        return trace.get_tracer(__name__)

    # Get configuration from environment
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    service_name = os.getenv("OTEL_SERVICE_NAME", service_name)
    service_version = os.getenv("OTEL_SERVICE_VERSION", "1.0.0")
    environment = os.getenv("ENVIRONMENT", "local")

    # Create resource with service information
    resource = Resource(attributes={
        SERVICE_NAME: service_name,
        SERVICE_VERSION: service_version,
        "deployment.environment": environment,
    })

    # Initialize tracer provider
    tracer_provider = TracerProvider(resource=resource)

    # Configure OTLP exporter (sends traces to SigNoz)
    otlp_exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint,
        insecure=True,  # Use True for local dev, False for production with TLS
    )

    # Add batch span processor for efficient export
    span_processor = BatchSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)

    # Set global tracer provider
    trace.set_tracer_provider(tracer_provider)

    # Auto-instrument database libraries
    try:
        PyMySQLInstrumentor().instrument()
        print("[✅] PyMySQL instrumentation enabled")
    except Exception as e:
        print(f"[⚠️] PyMySQL instrumentation failed: {e}")

    try:
        PymongoInstrumentor().instrument()
        print("[✅] PyMongo instrumentation enabled")
    except Exception as e:
        print(f"[⚠️] PyMongo instrumentation failed: {e}")

    print(f"[🔭] OpenTelemetry initialized: service={service_name}, endpoint={otlp_endpoint}")

    # Return tracer for manual instrumentation
    return trace.get_tracer(__name__)


def create_span_attributes(operation: str, **kwargs) -> dict:
    """
    Create standardized span attributes for database operations.

    Args:
        operation: Name of the operation (e.g., 'ingest', 'sync', 'validate')
        **kwargs: Additional attributes to include

    Returns:
        Dictionary of span attributes
    """
    attributes = {
        "operation": operation,
    }

    # Add all additional attributes
    for key, value in kwargs.items():
        # Convert to appropriate types
        if value is not None:
            if isinstance(value, (int, float, str, bool)):
                attributes[key] = value
            else:
                attributes[key] = str(value)

    return attributes


# Example usage functions for common patterns
def trace_mysql_operation(tracer: trace.Tracer, operation_name: str):
    """
    Context manager for tracing MySQL operations.

    Usage:
        with trace_mysql_operation(tracer, "insert_batch"):
            # MySQL operation code
    """
    return tracer.start_as_current_span(
        operation_name,
        attributes={"db.system": "mysql", "db.name": "nyc311"}
    )


def trace_mongo_operation(tracer: trace.Tracer, operation_name: str):
    """
    Context manager for tracing MongoDB operations.

    Usage:
        with trace_mongo_operation(tracer, "insert_many"):
            # MongoDB operation code
    """
    return tracer.start_as_current_span(
        operation_name,
        attributes={"db.system": "mongodb", "db.name": "nyc311"}
    )


if __name__ == "__main__":
    # Test telemetry setup
    print("Testing OpenTelemetry configuration...")

    tracer = setup_telemetry(service_name="telemetry-test")

    # Create a test span
    with tracer.start_as_current_span("test_operation") as span:
        span.set_attribute("test.key", "test_value")
        span.set_attribute("test.count", 42)
        print("[✅] Test span created successfully")

    print("\n[ℹ️] Telemetry test complete. Check SigNoz for traces.")
    print("[ℹ️] Note: SigNoz must be running at http://localhost:4317 to receive traces")
