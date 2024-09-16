package si.gaspervrhovsek;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import si.gaspervrhovsek.db.MatchEventRepository;
import si.gaspervrhovsek.models.MatchEvent;


@ApplicationScoped
public class DataStreamProcessor {
    private static Logger log = LoggerFactory.getLogger(DataStreamProcessor.class);

    @Inject
    MatchEventRepository matchEventRepository;

    public void processFile(final String filePath, final long startTimestamp) throws IOException {
        final var tempMap = new ConcurrentHashMap<String, MatchStreamProcessor>();

        Path path = Paths.get(filePath);
        final var lines = Files.lines(path).skip(1).toList();


        final var iterable = Multi.createFrom().iterable(lines);
        iterable.subscribe().with(
                line -> {
                    final var matchEvent = new MatchEvent(line);

                    tempMap.computeIfAbsent(matchEvent.getMatchId(), matchId -> new MatchStreamProcessor(matchEventRepository));
                    tempMap.get(matchEvent.getMatchId()).pushData(matchEvent);
                },
                error -> {
                    System.err.println("Error: " + error);
                    log.error("Error in subscribe with", error);
                },
                () -> {
                    System.out.println("All processed in " + (System.currentTimeMillis() - startTimestamp));
//                    final var allEvents = tempMap.entrySet().stream().map(stringListEntry -> stringListEntry.getValue().size()).reduce(Integer::sum);
//                    System.out.println("Processed " + tempMap.keySet().size() + " different matches with sum of all entries = " + allEvents.orElse(0));
                }
        );
    }
}
