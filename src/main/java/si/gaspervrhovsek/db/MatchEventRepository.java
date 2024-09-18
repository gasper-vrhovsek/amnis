package si.gaspervrhovsek.db;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import si.gaspervrhovsek.models.ExecutionResult;
import si.gaspervrhovsek.models.MatchEvent;

@ApplicationScoped
public class MatchEventRepository {
    private static final Logger log = LoggerFactory.getLogger(MatchEventRepository.class);

    private final PgPool client;

    @Inject
    public MatchEventRepository(final PgPool client) {
        this.client = client;
    }

    public void insertBatch(List<MatchEvent> matchEventList) {
        final var batch = matchEventList.stream().map(event -> Tuple.of(
                UUID.randomUUID(),
                event.getMatchId(),
                event.getMarketId(),
                event.getOutcomeId(),
                event.getSpecifiers(),
                LocalDateTime.now()
        )).toList();

        client.withTransaction(sqlConnection -> sqlConnection.preparedQuery(
                                "INSERT INTO match_events (id, match_id, market_id, outcome_id, specifiers, created_at) VALUES ($1, $2, $3, $4, $5, $6)")
                                                        .executeBatch(batch)
                                                        .onItem().invoke(() -> log.debug("Batch insert successful"))
                                                        .onFailure().invoke(err -> log.error("Batch insert error", err)))
                .await().indefinitely();
    }

    public ExecutionResult getExecutionResult() {
        return client.preparedQuery("SELECT min(created_at), max(created_at) FROM match_events").execute()
                .onItem().transform(rowSet -> {
                    final var iterator = rowSet.iterator();
                    if (iterator.hasNext()) {
                        final var result = iterator.next();
                        return new ExecutionResult(result.getLocalDateTime(0), result.getLocalDateTime(1));
                    } else {
                        return null;
                    }
                }).await().indefinitely();
    }

    public void deleteAll() {
        client.preparedQuery("DELETE FROM public.match_events").execute().onItem().invoke(() -> {
            log.info("Deleted all records from public.match_events");
        }).onFailure().invoke(err -> {
            log.error("Could not delete all records from public.match_events", err);
        });
    }
}
