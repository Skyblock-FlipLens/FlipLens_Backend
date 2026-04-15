package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.model.DataSourceHash;
import com.skyblockflipper.backend.model.ElectionSnapshot;
import com.skyblockflipper.backend.repository.DataSourceHashRepository;
import com.skyblockflipper.backend.repository.ElectionSnapshotRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

@Component
@Slf4j
public class ElectionPollFreshnessService {

    static final String ELECTION_POLL_SOURCE_KEY = "HYPIXEL-ELECTION-POLL";
    private static final String ELECTION_UNAVAILABLE_HASH = "ELECTION_UNAVAILABLE";
    // SkyBlock mayor election cycle is 124 in-game hours.
    private static final Duration DEFAULT_MAX_AGE = Duration.ofHours(124);

    private final DataSourceHashRepository dataSourceHashRepository;
    private final ElectionSnapshotRepository electionSnapshotRepository;
    private final HypixelClient hypixelClient;
    private final long maxAgeMillis;
    private final Object lock = new Object();

    public ElectionPollFreshnessService(DataSourceHashRepository dataSourceHashRepository,
                                        ElectionSnapshotRepository electionSnapshotRepository,
                                        HypixelClient hypixelClient,
                                        @Value("${config.hypixel.polling.election-max-age:PT124H}") Duration maxAge) {
        this.dataSourceHashRepository = dataSourceHashRepository;
        this.electionSnapshotRepository = electionSnapshotRepository;
        this.hypixelClient = hypixelClient;
        this.maxAgeMillis = sanitizeDuration(maxAge, DEFAULT_MAX_AGE);
    }

    public void ensureRecentElectionPoll() {
        synchronized (lock) {
            Instant now = Instant.now();
            DataSourceHash existing = dataSourceHashRepository.findBySourceKey(ELECTION_POLL_SOURCE_KEY);

            JsonNode electionPayload = null;
            try {
                electionPayload = hypixelClient.fetchElection();
            } catch (RuntimeException e) {
                log.warn("Election poll refresh failed: {}", e.getMessage());
            }

            if (electionPayload != null) {
                String hash = sha256(electionPayload.toString());
                if (existing == null || !hash.equals(existing.getHash())) {
                    electionSnapshotRepository.save(new ElectionSnapshot(now, hash, electionPayload.toString()));
                }
                upsertHash(existing, hash, now);
                return;
            }

            if (isRecent(existing, now)) {
                return;
            }

            String hash = resolveHash(existing, electionPayload);
            upsertHash(existing, hash, now);
        }
    }

    private boolean isRecent(DataSourceHash sourceHash, Instant now) {
        if (sourceHash == null || sourceHash.getUpdatedAt() == null) {
            return false;
        }
        return !now.isAfter(sourceHash.getUpdatedAt().plusMillis(maxAgeMillis));
    }

    private void upsertHash(DataSourceHash existing, String hash, Instant now) {
        if (existing == null) {
            DataSourceHash created = new DataSourceHash(ELECTION_POLL_SOURCE_KEY, hash);
            created.setUpdatedAt(now);
            dataSourceHashRepository.save(created);
            return;
        }
        existing.setHash(hash);
        existing.setUpdatedAt(now);
        dataSourceHashRepository.save(existing);
    }

    private String resolveHash(DataSourceHash existing, JsonNode payload) {
        if (payload != null) {
            return sha256(payload.toString());
        }
        if (existing != null && existing.getHash() != null && !existing.getHash().isBlank()) {
            return existing.getHash();
        }
        return ELECTION_UNAVAILABLE_HASH;
    }

    private long sanitizeDuration(Duration configured, Duration fallback) {
        Duration safe = configured == null || configured.isNegative() || configured.isZero() ? fallback : configured;
        return safe.toMillis();
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
