package si.gaspervrhovsek;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.transaction.Transactional;
import si.gaspervrhovsek.db.MatchEventJpa;
import si.gaspervrhovsek.db.MatchEventRepository;
import si.gaspervrhovsek.models.MatchEvent;

public class MatchStreamProcessor {
    private final UnicastProcessor<MatchEvent> processor;
    private final AtomicBoolean isProcessing;
    private final MatchEventRepository repository;


    public MatchStreamProcessor(final MatchEventRepository matchEventRepository) {
        this.processor = UnicastProcessor.create();
        this.isProcessing = new AtomicBoolean(false);
        this.repository = matchEventRepository;

        startProcessing();
    }

    public void pushData(MatchEvent matchEvent) {
        processor.onNext(matchEvent);
    }

    private void startProcessing() {
        if (isProcessing.compareAndSet(false, true)) {
            processor.onItem().transformToUniAndMerge(this::processData)
                    .subscribe().with(
                            success -> {},
                            failure -> System.err.println("Error processing data: " + failure)
                    );
        }
    }

    private Uni<Void> processData(MatchEvent matchEvent) {
        return Uni.createFrom().voidItem().onItem().invoke(() -> {
            insertNewMatchEvent(matchEvent);
        });
    }

    private void insertNewMatchEvent(final MatchEvent matchEvent) {
        final var matchEventJpa = new MatchEventJpa();
        matchEventJpa.id = UUID.randomUUID();
        matchEventJpa.matchId = matchEvent.getMatchId();
        matchEventJpa.marketId = matchEvent.getMarketId();
        matchEventJpa.outcomeId = matchEvent.getOutcomeId();
        matchEventJpa.specifiers = matchEvent.getSpecifiers();
        matchEventJpa.createdAt = Instant.now();

        repository.insert(matchEventJpa);
    }
}
