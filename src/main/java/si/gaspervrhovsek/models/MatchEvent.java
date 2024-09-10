package si.gaspervrhovsek.models;

public class MatchEvent {

    private String matchId;
    private int marketId;
    private String outcomeId;
    private String specifiers;

    public MatchEvent(String line) {
        // TODO throw logic to a parser class. For now it's ok.
        final var split = line.split("\\|");
        if (split.length >= 3) {
            matchId = split[0].replaceAll("'", "");
            marketId = Integer.parseInt(split[1]);
            outcomeId = split[2].replaceAll("'", "");
        }
        if (split.length > 3) {
            // TODO parse specifiers
        }
    }

    public String getMatchId() {
        return matchId;
    }

    public int getMarketId() {
        return marketId;
    }

    public String getOutcomeId() {
        return outcomeId;
    }

    public String getSpecifiers() {
        return specifiers;
    }
}
