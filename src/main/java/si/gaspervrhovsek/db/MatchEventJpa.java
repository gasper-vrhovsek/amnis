package si.gaspervrhovsek.db;

import java.time.Instant;
import java.util.UUID;

import org.hibernate.annotations.CreationTimestamp;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "match_events")
public class MatchEventJpa extends PanacheEntityBase {

    @Id
    public UUID id;

    @Column(name = "match_id")
    public String matchId;

    @Column(name = "market_id")
    public int marketId;
    @Column(name = "outcome_id")
    public String outcomeId;
    @Column(name = "specifiers")
    public String specifiers;

    @CreationTimestamp
    @Column(name = "created_at")
    public Instant createdAt;
}
