package si.gaspervrhovsek;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import si.gaspervrhovsek.db.MatchEventRepository;
import si.gaspervrhovsek.models.MatchEvent;

public class MatchStreamProcessor {
    private static final Logger log = LoggerFactory.getLogger(MatchStreamProcessor.class);

    private final UnicastProcessor<MatchEvent> processor;
    private final AtomicBoolean isProcessing;
    private final MatchEventRepository repository;

    private final List<MatchEvent> batch;
    private final AtomicInteger batchSize;
    private static final int BATCH_SIZE_LIMIT = 1000;

    public MatchStreamProcessor(final MatchEventRepository matchEventRepository) {
        this.processor = UnicastProcessor.create();
        this.isProcessing = new AtomicBoolean(false);
        this.repository = matchEventRepository;

        this.batch = new ArrayList<>();
        this.batchSize = new AtomicInteger(0);

        startProcessing();
    }

    public void pushData(MatchEvent matchEvent) {
        processor.onNext(matchEvent);
    }

    public void completeProcessing() {
        processor.onComplete();
    }

    private void startProcessing() {
        if (isProcessing.compareAndSet(false, true)) {
            processor.onItem().transformToUniAndMerge(this::processData)
                    .onCompletion().invoke(this::flushBatch)
                    .subscribe().with(
                            success -> {
                            },
                            failure -> System.err.println("Error processing data: " + failure)
                    );
        }
    }

    private Uni<Void> processData(MatchEvent matchEvent) {
        return Uni.createFrom().voidItem().onItem().invoke(() -> {
            synchronized (batch) {
                batch.add(matchEvent);
                if (batchSize.incrementAndGet() >= BATCH_SIZE_LIMIT) {
                    flushBatch();
                }
            }
        });
    }

    private void flushBatch() {
        synchronized (batch) {
            insertMatchEvents(batch);
            batch.clear();
            batchSize.set(0);
        }
    }

    private void insertMatchEvents(final List<MatchEvent> eventsToInsert) {
        if (eventsToInsert.isEmpty()) {
            return;
        }
        repository.insertBatch(eventsToInsert);
    }
}
