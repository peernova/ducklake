package io.ducklake.service.event.aspect;

import io.ducklake.service.event.annotation.AccessLog;
import io.ducklake.service.event.model.AccessEvent;
import io.ducklake.service.event.model.Resource;
import io.ducklake.service.event.service.EventService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * AOP aspect for automatic access logging.
 *
 * Uses @AccessLog annotation for explicit resource type configuration.
 * Falls back to inference from path variables if annotation not present.
 */
@Aspect
@Component
@RequiredArgsConstructor
@Slf4j
public class AccessLoggingAspect {

    private final EventService eventService;

    private static final String USER_ID_HEADER = "X-User-Id";
    private static final String USER_EMAIL_HEADER = "X-User-Email";
    private static final String DEFAULT_USER = "anonymous";

    @Around("execution(* io.ducklake.service.controller..*(..)) && !execution(* io.ducklake.service.event.controller..*(..))")
    public Object logAccess(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();

        // Check for @AccessLog annotation
        AccessLog accessLog = method.getAnnotation(AccessLog.class);

        // Skip if explicitly marked
        if (accessLog != null && accessLog.skip()) {
            return joinPoint.proceed();
        }

        long startTime = System.currentTimeMillis();
        HttpServletRequest request = getCurrentRequest();

        // Extract context
        String userId = extractUserId(request);
        String userEmail = extractUserEmail(request);
        String sourceIp = extractSourceIp(request);
        String clientInfo = extractClientInfo(request);
        Map<String, String> pathParams = extractPathParams(joinPoint);
        Resource resource = extractResource(accessLog, pathParams, request);
        String operation = extractOperation(accessLog, method, request);

        String status = "success";
        String errorReason = null;
        Long rowsAffected = null;

        try {
            Object result = joinPoint.proceed();

            if (result instanceof ResponseEntity<?> response) {
                if (!response.getStatusCode().is2xxSuccessful()) {
                    status = "error";
                    errorReason = "HTTP " + response.getStatusCode().value();
                }
                rowsAffected = extractRowCount(response.getBody());
            }

            return result;

        } catch (Exception e) {
            status = "error";
            errorReason = e.getClass().getSimpleName() + ": " + truncate(e.getMessage(), 200);
            throw e;

        } finally {
            long executionTime = System.currentTimeMillis() - startTime;

            try {
                AccessEvent event = AccessEvent.builder()
                        .timestamp(Instant.now())
                        .userId(userId)
                        .userEmail(userEmail)
                        .sourceIp(sourceIp)
                        .clientInfo(clientInfo)
                        .resource(resource)
                        .operation(operation)
                        .status(status)
                        .rejectionReason(errorReason)
                        .rowsAffected(rowsAffected)
                        .executionTimeMs((int) executionTime)
                        .build();

                eventService.recordEvent(event);

                if (log.isDebugEnabled()) {
                    log.debug("Access logged: {} {} {}/{} in {}ms",
                            userId, operation, resource.getType(), resource.getId(), executionTime);
                }
            } catch (Exception e) {
                log.warn("Failed to log access event: {}", e.getMessage());
            }
        }
    }

    private Map<String, String> extractPathParams(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();
        Parameter[] params = method.getParameters();

        Map<String, String> pathParams = new HashMap<>();
        for (int i = 0; i < params.length; i++) {
            PathVariable pathVar = params[i].getAnnotation(PathVariable.class);
            if (pathVar != null && args[i] != null) {
                String name = pathVar.value().isEmpty() ? params[i].getName() : pathVar.value();
                pathParams.put(name, args[i].toString());
            }
        }
        return pathParams;
    }

    private Resource extractResource(AccessLog accessLog, Map<String, String> pathParams, HttpServletRequest request) {
        String resourceType;
        String resourceId;
        String resourceName = null;

        if (accessLog != null && !accessLog.resourceType().isEmpty()) {
            // Use annotation values
            resourceType = accessLog.resourceType();

            // Build resource ID from specified params
            if (accessLog.resourceIdParams().length > 0) {
                StringJoiner idJoiner = new StringJoiner("/");
                for (String param : accessLog.resourceIdParams()) {
                    String value = pathParams.get(param);
                    if (value != null) {
                        idJoiner.add(value);
                    }
                }
                resourceId = idJoiner.toString();
            } else {
                resourceId = request != null ? request.getRequestURI() : "unknown";
            }

            // Extract display name if specified
            if (!accessLog.resourceNameParam().isEmpty()) {
                resourceName = pathParams.get(accessLog.resourceNameParam());
            }
        } else {
            // Fallback: infer from path params
            resourceType = inferResourceType(pathParams);
            resourceId = inferResourceId(pathParams, resourceType);
            resourceName = inferResourceName(pathParams, resourceType);
        }

        return Resource.builder()
                .type(resourceType)
                .id(resourceId)
                .name(resourceName)
                .path(pathParams.isEmpty() ? null : pathParams)
                .build();
    }

    private String inferResourceType(Map<String, String> pathParams) {
        // Infer from most specific path param present
        if (pathParams.containsKey("tableName")) return "table";
        if (pathParams.containsKey("schemaName")) return "schema";
        if (pathParams.containsKey("branchName")) return "branch";
        if (pathParams.containsKey("catalogId")) return "catalog";
        return "api";
    }

    private String inferResourceId(Map<String, String> pathParams, String resourceType) {
        return switch (resourceType) {
            case "table" -> String.join("/",
                    pathParams.getOrDefault("catalogId", ""),
                    pathParams.getOrDefault("schemaName", ""),
                    pathParams.getOrDefault("tableName", ""));
            case "schema" -> String.join("/",
                    pathParams.getOrDefault("catalogId", ""),
                    pathParams.getOrDefault("schemaName", ""));
            case "branch" -> String.join("/",
                    pathParams.getOrDefault("catalogId", ""),
                    pathParams.getOrDefault("branchName", ""));
            case "catalog" -> pathParams.getOrDefault("catalogId", "");
            default -> "";
        };
    }

    private String inferResourceName(Map<String, String> pathParams, String resourceType) {
        return switch (resourceType) {
            case "table" -> pathParams.get("tableName");
            case "schema" -> pathParams.get("schemaName");
            case "branch" -> pathParams.get("branchName");
            case "catalog" -> pathParams.get("catalogId");
            default -> null;
        };
    }

    private String extractOperation(AccessLog accessLog, Method method, HttpServletRequest request) {
        // Use annotation if specified
        if (accessLog != null && !accessLog.operation().isEmpty()) {
            return accessLog.operation();
        }

        // Derive from HTTP method
        if (method.isAnnotationPresent(GetMapping.class)) {
            return "view";
        } else if (method.isAnnotationPresent(PostMapping.class)) {
            String methodName = method.getName().toLowerCase();
            if (methodName.contains("execute") || methodName.contains("query")) return "execute";
            if (methodName.contains("search")) return "search";
            if (methodName.contains("test")) return "test";
            if (methodName.contains("use")) return "switch";
            return "create";
        } else if (method.isAnnotationPresent(PutMapping.class)) {
            return "update";
        } else if (method.isAnnotationPresent(DeleteMapping.class)) {
            return "delete";
        } else if (method.isAnnotationPresent(PatchMapping.class)) {
            return "update";
        }

        return request != null ? request.getMethod().toLowerCase() : "unknown";
    }

    private HttpServletRequest getCurrentRequest() {
        ServletRequestAttributes attrs = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        return attrs != null ? attrs.getRequest() : null;
    }

    private String extractUserId(HttpServletRequest request) {
        if (request == null) return DEFAULT_USER;
        String userId = request.getHeader(USER_ID_HEADER);
        return userId != null ? userId : DEFAULT_USER;
    }

    private String extractUserEmail(HttpServletRequest request) {
        return request != null ? request.getHeader(USER_EMAIL_HEADER) : null;
    }

    private String extractSourceIp(HttpServletRequest request) {
        if (request == null) return null;
        String ip = request.getHeader("X-Forwarded-For");
        if (ip != null && !ip.isEmpty()) return ip.split(",")[0].trim();
        ip = request.getHeader("X-Real-IP");
        if (ip != null && !ip.isEmpty()) return ip;
        return request.getRemoteAddr();
    }

    private String extractClientInfo(HttpServletRequest request) {
        return request != null ? request.getHeader("User-Agent") : null;
    }

    private Long extractRowCount(Object body) {
        if (body == null) return null;
        try {
            var method = body.getClass().getMethod("getRowCount");
            Object result = method.invoke(body);
            if (result instanceof Number n) return n.longValue();
        } catch (Exception ignored) {}
        return null;
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() > maxLen ? s.substring(0, maxLen) + "..." : s;
    }
}
