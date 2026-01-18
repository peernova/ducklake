package io.ducklake.service.filter;

import io.ducklake.service.tracing.TraceContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.IOException;
import java.util.UUID;

/**
 * Filter that adds traceId and spanId headers to all HTTP responses.
 * This enables distributed tracing correlation for both successful and error responses.
 *
 * Runs at HIGHEST_PRECEDENCE so headers are added BEFORE response is committed.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
@RequiredArgsConstructor
@Slf4j
public class TraceResponseFilter extends OncePerRequestFilter {

    private static final String TRACE_ID_HEADER = "X-Trace-Id";
    private static final String SPAN_ID_HEADER = "X-Span-Id";
    private static final String REQUEST_ID_HEADER = "X-Request-Id";
    private static final String REQUEST_ID_ATTRIBUTE = "ducklake.requestId";

    private final TraceContext traceContext;

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        // Generate request ID early
        String requestId = generateRequestId(request);
        request.setAttribute(REQUEST_ID_ATTRIBUTE, requestId);

        // Set request ID header immediately (before filter chain)
        response.setHeader(REQUEST_ID_HEADER, requestId);

        // Wrap response to buffer content and allow header modification
        ContentCachingResponseWrapper responseWrapper = new ContentCachingResponseWrapper(response);

        try {
            filterChain.doFilter(request, responseWrapper);
        } finally {
            // Add trace headers before copying body to response
            addTraceHeaders(responseWrapper, requestId);

            // Copy cached content to the original response
            responseWrapper.copyBodyToResponse();
        }
    }

    private String generateRequestId(HttpServletRequest request) {
        // Check if client sent a request ID
        String clientRequestId = request.getHeader("X-Request-Id");
        if (clientRequestId != null && !clientRequestId.isEmpty()) {
            return clientRequestId;
        }
        // Generate a new UUID-based request ID
        return UUID.randomUUID().toString().replace("-", "");
    }

    private void addTraceHeaders(HttpServletResponse response, String requestId) {
        // Try to get trace ID from tracing context
        String traceId = traceContext.getTraceId();
        String spanId = traceContext.getSpanId();

        // Fall back to request ID if no trace context available
        if (traceId == null || traceId.isEmpty()) {
            traceId = requestId;
        }

        if (spanId == null || spanId.isEmpty()) {
            // Generate a short span ID if none available
            spanId = requestId.length() > 16 ? requestId.substring(0, 16) : requestId;
        }

        // Set headers
        response.setHeader(TRACE_ID_HEADER, traceId);
        response.setHeader(SPAN_ID_HEADER, spanId);
        response.setHeader(REQUEST_ID_HEADER, requestId);

        log.debug("Added trace headers: traceId={}, spanId={}, requestId={}", traceId, spanId, requestId);
    }
}
