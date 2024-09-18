package si.gaspervrhovsek.models;

import java.time.LocalDateTime;

public record ExecutionResult(LocalDateTime minCreatedAt, LocalDateTime maxCreatedAt) {
}
