package io.ducklake.service.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Utility to access current trace context.
 */
@Component
@RequiredArgsConstructor
public class TraceContext {

    private final Tracer tracer;

    public String getTraceId() {
        Span span = tracer.currentSpan();
        if (span != null) {
            return span.context().traceId();
        }
        return null;
    }

    public String getSpanId() {
        Span span = tracer.currentSpan();
        if (span != null) {
            return span.context().spanId();
        }
        return null;
    }

    public String getParentSpanId() {
        Span span = tracer.currentSpan();
        if (span != null && span.context().parentId() != null) {
            return span.context().parentId();
        }
        return null;
    }

    public Tracer getTracer() {
        return tracer;
    }

    /**
     * Create a new span as child of current span.
     */
    public Span startSpan(String name) {
        return tracer.nextSpan().name(name).start();
    }
}
