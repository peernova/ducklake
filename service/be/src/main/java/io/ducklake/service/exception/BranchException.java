package io.ducklake.service.exception;

import io.ducklake.service.model.dto.ErrorCode;
import io.ducklake.service.model.dto.ErrorDetail;

import java.util.Collections;

/**
 * Exception for branch-related errors.
 */
public class BranchException extends DuckLakeException {

    public BranchException(String message) {
        super(ErrorCode.INVALID_ARGUMENT, message);
    }

    public BranchException(String message, Throwable cause) {
        super(ErrorCode.INVALID_ARGUMENT, message, cause);
    }

    public static BranchException notFound(String catalogId, String branchName) {
        return new BranchException(ErrorCode.NOT_FOUND,
                "Branch not found: " + branchName,
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.BranchError")
                                .field("branchName")
                                .description("No branch '" + branchName + "' exists in catalog '" + catalogId + "'")
                                .build()
                ));
    }

    public static BranchException alreadyExists(String catalogId, String branchName) {
        return new BranchException(ErrorCode.ALREADY_EXISTS,
                "Branch already exists: " + branchName,
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.BranchError")
                                .field("branchName")
                                .description("A branch named '" + branchName + "' already exists in catalog '" + catalogId + "'")
                                .build()
                ));
    }

    public static BranchException mergeFailed(String sourceBranch, String targetBranch, String reason) {
        return new BranchException(ErrorCode.FAILED_PRECONDITION,
                "Merge failed: " + sourceBranch + " -> " + targetBranch,
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.BranchError")
                                .description(reason)
                                .build()
                ));
    }

    public static BranchException mergeConflict(String sourceBranch, String targetBranch, String conflictDetails) {
        return new BranchException(ErrorCode.ABORTED,
                "Merge conflict detected",
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.MergeConflict")
                                .description("Conflict merging '" + sourceBranch + "' into '" + targetBranch + "': " + conflictDetails)
                                .build()
                ));
    }

    public static BranchException invalidBranchName(String branchName, String reason) {
        return new BranchException(ErrorCode.INVALID_ARGUMENT,
                "Invalid branch name: " + branchName,
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.BranchError")
                                .field("branchName")
                                .description(reason)
                                .build()
                ));
    }

    private BranchException(ErrorCode code, String message, java.util.List<ErrorDetail> details) {
        super(code, message, details);
    }
}
