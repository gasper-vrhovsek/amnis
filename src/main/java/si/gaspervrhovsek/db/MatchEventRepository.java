package si.gaspervrhovsek.db;

import java.util.List;

import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class MatchEventRepository implements PanacheRepository<MatchEventJpa> {
    private final EntityManager entityManager;

    @Inject
    public MatchEventRepository(final EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Transactional
    public void insert(MatchEventJpa matchEventJpa) {
        entityManager.persist(matchEventJpa);
        entityManager.flush();
    }

    @Transactional
    public void insertBatch(List<MatchEventJpa> matchEventJpas) {
        for (MatchEventJpa jpa : matchEventJpas) {
            entityManager.persist(jpa);
        }
        entityManager.flush();
    }
}
