package si.gaspervrhovsek;

import java.io.IOException;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;

@QuarkusMain
public class Main implements QuarkusApplication {
    @Inject
    DataStreamProcessor dataStreamProcessor;

    @Override
    public int run(final String... args) throws IOException {
        final var filePath = "src/main/resources/fo_random.txt";
        final var timestamp = System.currentTimeMillis();
        dataStreamProcessor.processFile(filePath, timestamp);
        return 0;
    }
}