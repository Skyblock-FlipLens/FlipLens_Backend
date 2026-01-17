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
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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


    @Test
    void loadItemJsonsReadsItemsWhenRefreshNotDue() throws Exception {
        Path itemsDir = createItemsDir();

        ReflectionTestUtils.setField(client, "itemsDir", itemsDir);

        DataSourceHashRepository repository = mock(DataSourceHashRepository.class);
        when(repository.findBySourceKey("NEU-ITEMS"))
                .thenReturn(new DataSourceHash(null, "NEU-ITEMS", "hash", Instant.now()));

        ReflectionTestUtils.setField(client, "dataSourceHashRepository", repository);

        Set<String> ids = client.loadItemJsons().stream()
                .map(node -> node.get("id").asString())
                .collect(java.util.stream.Collectors.toSet());

        assertEquals(Set.of("a", "b"), ids);
        verify(repository, never()).save(any());
    }

    @Test
    void resolveZipUrlSupportsGitHubAndZipUrls() {
        String github = ReflectionTestUtils.invokeMethod(client, "resolveZipUrl",
                "https://github.com/owner/repo", "main");
        String githubWithSlash = ReflectionTestUtils.invokeMethod(client, "resolveZipUrl",
                "https://github.com/owner/repo/", "dev");
        String directZip = ReflectionTestUtils.invokeMethod(client, "resolveZipUrl",
                "https://example.com/repo.zip", "ignored");

        assertEquals("https://codeload.github.com/owner/repo/zip/refs/heads/main", github);
        assertEquals("https://codeload.github.com/owner/repo/zip/refs/heads/dev", githubWithSlash);
        assertEquals("https://example.com/repo.zip", directZip);
    }

    @Test
    void resolveZipUrlRejectsUnsupportedUrl() {
        assertThrows(IllegalArgumentException.class, () -> ReflectionTestUtils.invokeMethod(
                client, "resolveZipUrl", "https://example.com/repo", "main"));
    }

    @Test
    void deleteDirectoryRemovesNestedFiles() throws Exception {
        Path dir = tempDir.resolve("to-delete");
        Files.createDirectories(dir.resolve("nested"));
        Files.writeString(dir.resolve("nested").resolve("a.json"), "{\"id\":\"a\"}");

        ReflectionTestUtils.invokeMethod(client, "deleteDirectory", dir);

        assertFalse(Files.exists(dir));
    }

    @Test
    void isRefreshDueRespectsZeroRefreshWindow() {
        NEUClient zeroRefreshClient = new NEUClient("https://github.com/owner/repo",
                tempDir.toString(), "main", 0);
        Boolean due = ReflectionTestUtils.invokeMethod(zeroRefreshClient, "isRefreshDue",
                Instant.now(), Instant.now());

        assertNotNull(due);
        assertTrue(due);
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
