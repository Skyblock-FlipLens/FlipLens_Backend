package com.skyblockflipper.backend.NEU;

import com.skyblockflipper.backend.model.DataSourceHash;
import com.skyblockflipper.backend.repository.DataSourceHashRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(properties = "config.NEU.refresh-days=3")
class NEUClientTest {

    @TempDir
    Path tempDir;
    @Autowired
    NEUClient client;

    @Test
    void updateHashReturnsFalseWhenUpToDate() throws Exception {
        Path itemsDir = createItemsDir();
        String expectedHash = computeExpectedHash(itemsDir);

        ReflectionTestUtils.setField(client, "itemsDir", itemsDir);

        DataSourceHashRepository repository = mock(DataSourceHashRepository.class);
        when(repository.findBySourceKey("NEU-ITEMS"))
                .thenReturn(new DataSourceHash(null, "NEU-ITEMS", expectedHash, Instant.now()));

        ReflectionTestUtils.setField(client, "dataSourceHashRepository", repository);

        boolean updated = client.updateHash();

        assertFalse(updated);
        verify(repository, never()).save(any());
    }

    @Test
    void updateHashSkipsWhenWithinRefreshWindow() throws Exception {
        Path itemsDir = createItemsDir();
        String expectedHash = computeExpectedHash(itemsDir);

        ReflectionTestUtils.setField(client, "itemsDir", itemsDir);

        DataSourceHashRepository repository = mock(DataSourceHashRepository.class);
        when(repository.findBySourceKey("NEU-ITEMS"))
                .thenReturn(new DataSourceHash(null, "NEU-ITEMS", expectedHash, Instant.now().minusSeconds(60 * 60 * 24)));

        ReflectionTestUtils.setField(client, "dataSourceHashRepository", repository);

        boolean updated = client.updateHash();

        assertFalse(updated);
        verify(repository, never()).save(any());
    }

    @Test
    void updateHashSavesWhenMissing() throws Exception {
        Path itemsDir = createItemsDir();
        String expectedHash = computeExpectedHash(itemsDir);

        ReflectionTestUtils.setField(client, "itemsDir", itemsDir);

        DataSourceHashRepository repository = mock(DataSourceHashRepository.class);
        when(repository.findBySourceKey("NEU-ITEMS")).thenReturn(null);

        ReflectionTestUtils.setField(client, "dataSourceHashRepository", repository);

        boolean updated = client.updateHash();

        assertTrue(updated);
        ArgumentCaptor<DataSourceHash> captor = ArgumentCaptor.forClass(DataSourceHash.class);
        verify(repository).save(captor.capture());
        DataSourceHash saved = captor.getValue();
        assertEquals("NEU-ITEMS", saved.getSourceKey());
        assertEquals(expectedHash, saved.getHash());
    }

    private Path createItemsDir() throws IOException {
        Path itemsDir = tempDir.resolve("items");
        Files.createDirectories(itemsDir.resolve("nested"));
        Files.writeString(itemsDir.resolve("nested").resolve("a.json"), "{\"id\":\"a\"}");
        Files.writeString(itemsDir.resolve("b.json"), "{\"id\":\"b\"}");
        return itemsDir;
    }

    private String computeExpectedHash(Path dir) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.filter(path -> Files.isRegularFile(path)
                            && path.getFileName().toString().endsWith(".json"))
                    .map(dir::relativize)
                    .map(Path::toString)
                    .sorted()
                    .forEachOrdered(relative -> {
                        try {
                            digest.update(relative.getBytes(StandardCharsets.UTF_8));
                            Path filePath = dir.resolve(relative);
                            byte[] bytes = Files.readAllBytes(filePath);
                            digest.update(bytes, 0, bytes.length);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return toHex(digest.digest());
    }

    private String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
