package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.model.DataSourceHash;
import com.skyblockflipper.backend.repository.DataSourceHashRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ElectionPollFreshnessServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void ensureRecentElectionPollSkipsFetchWhenHashIsFresh() {
        DataSourceHashRepository repository = mock(DataSourceHashRepository.class);
        HypixelClient hypixelClient = mock(HypixelClient.class);
        DataSourceHash existing = new DataSourceHash(
                null,
                ElectionPollFreshnessService.ELECTION_POLL_SOURCE_KEY,
                "existing-hash",
                Instant.now()
        );
        when(repository.findBySourceKey(ElectionPollFreshnessService.ELECTION_POLL_SOURCE_KEY)).thenReturn(existing);
        ElectionPollFreshnessService service = new ElectionPollFreshnessService(repository, hypixelClient, Duration.ofMinutes(5));

        service.ensureRecentElectionPoll();

        verify(hypixelClient, never()).fetchElection();
        verify(repository, never()).save(any(DataSourceHash.class));
    }

    @Test
    void ensureRecentElectionPollFetchesAndSavesHashWhenStaleOrMissing() throws Exception {
        DataSourceHashRepository repository = mock(DataSourceHashRepository.class);
        HypixelClient hypixelClient = mock(HypixelClient.class);
        JsonNode payload = objectMapper.readTree("""
                {
                  "success": true,
                  "mayor": { "name": "Derpy" }
                }
                """);
        when(repository.findBySourceKey(ElectionPollFreshnessService.ELECTION_POLL_SOURCE_KEY)).thenReturn(null);
        when(hypixelClient.fetchElection()).thenReturn(payload);
        ElectionPollFreshnessService service = new ElectionPollFreshnessService(repository, hypixelClient, Duration.ofMinutes(5));

        service.ensureRecentElectionPoll();

        ArgumentCaptor<DataSourceHash> captor = ArgumentCaptor.forClass(DataSourceHash.class);
        verify(repository).save(captor.capture());
        DataSourceHash saved = captor.getValue();
        assertEquals(ElectionPollFreshnessService.ELECTION_POLL_SOURCE_KEY, saved.getSourceKey());
        assertEquals(sha256(payload.toString()), saved.getHash());
        assertTrue(saved.getUpdatedAt().isAfter(Instant.now().minusSeconds(5)));
    }

    @Test
    void ensureRecentElectionPollPreservesExistingHashWhenFetchFails() {
        DataSourceHashRepository repository = mock(DataSourceHashRepository.class);
        HypixelClient hypixelClient = mock(HypixelClient.class);
        Instant original = Instant.now().minus(Duration.ofHours(1));
        DataSourceHash existing = new DataSourceHash(
                null,
                ElectionPollFreshnessService.ELECTION_POLL_SOURCE_KEY,
                "old-hash",
                original
        );
        when(repository.findBySourceKey(ElectionPollFreshnessService.ELECTION_POLL_SOURCE_KEY)).thenReturn(existing);
        when(hypixelClient.fetchElection()).thenReturn(null);
        ElectionPollFreshnessService service = new ElectionPollFreshnessService(repository, hypixelClient, Duration.ofMinutes(5));

        service.ensureRecentElectionPoll();

        verify(repository).save(existing);
        assertEquals("old-hash", existing.getHash());
        assertTrue(existing.getUpdatedAt().isAfter(original));
    }

    private String sha256(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    }
}
