package si.gaspervrhovsek;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.smallrye.context.SmallRyeContextManagerProvider;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import si.gaspervrhovsek.models.MatchEvent;

@QuarkusMain
public class Main implements QuarkusApplication {

    @Inject
    Vertx vertx;

    @Override
    public int run(final String... args) {
        final var filePath = "src/main/resources/fo_random.txt";
        final var timestamp = System.currentTimeMillis();
        final var tempMap = new ConcurrentHashMap<String, List<MatchEvent>>();

        final var uni = Uni.createFrom().item(SmallRyeContextManagerProvider::getManager)
                                .onItem().ifNull().failWith(new RuntimeException("Context Manager not initialized"))
                                .onItem().transformToUni(
                                        manager ->
                                                Uni.createFrom().completionStage(vertx.fileSystem().readFile(filePath).toCompletionStage()));

        final var linesMulti = uni.onItem().transformToMulti(fileBuffer -> Multi.createFrom().items(() -> fileBuffer.toString().lines().skip(1)));q

        linesMulti.subscribe().with(
                line -> {
                    final var matchEvent = new MatchEvent(line);

                    tempMap.computeIfAbsent(matchEvent.getMatchId(), matchId -> new ArrayList<>());
                    tempMap.get(matchEvent.getMatchId()).add(matchEvent);
                },
                error -> System.err.println("Error: " + error),
                () -> {
                    System.out.println("All processed in " + (System.currentTimeMillis() - timestamp));
                    System.out.println("Processed " + tempMap.keySet().size() + " different matches");
                }
        );
        return 0;
    }


}
