package com.skyblockflipper.backend.NEU;

import com.skyblockflipper.backend.model.DataSourceHash;
import com.skyblockflipper.backend.repository.DataSourceHashRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;
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
                .map(node -> {
                    String id = getString(node.path("id"));
                    if (id.isBlank()) {
                        id = getString(node.path("internalname"));
                    }
                    return id;
                })
                .collect(java.util.stream.Collectors.toSet());

        assertEquals(Set.of("ARMOR_OF_YOG_BOOTS", "ARMADILLO;5"), ids);
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
    void fetchDataSkipsWhenItemsAlreadyPresent() throws Exception {
        Path itemsDir = tempDir.resolve("existing-items");
        Files.createDirectories(itemsDir);
        Files.writeString(itemsDir.resolve("already.json"), "{\"id\":\"x\"}");

        NEUClient localClient = new NEUClient("https://example.com/repo.zip",
                itemsDir.toString(), "main", 1, new NEUItemFilterHandler());

        localClient.fetchData();

        assertTrue(Files.exists(itemsDir.resolve("already.json")));
    }

    @Test
    void downloadAndExtractItemsDownloadsZipWithItems() throws Exception {
        byte[] zipBytes = buildZip(Map.of(
                "repo/items/ITEM_A.json", "{\"id\":\"ITEM_A\"}",
                "repo/README.txt", "ignore"
        ));
        HttpServer server = startZipServer(zipBytes);
        String url = "http://localhost:" + server.getAddress().getPort() + "/repo.zip";

        Path itemsDir = tempDir.resolve("downloaded-items");
        Files.createDirectories(itemsDir);

        try {
            ReflectionTestUtils.invokeMethod(client, "downloadAndExtractItems", url, "main", itemsDir);
        } finally {
            server.stop(0);
        }

        assertTrue(Files.exists(itemsDir.resolve("ITEM_A.json")));
    }

    @Test
    void downloadAndExtractItemsThrowsWhenNoItemJsons() throws Exception {
        byte[] zipBytes = buildZip(Map.of(
                "repo/README.txt", "no items here"
        ));
        HttpServer server = startZipServer(zipBytes);
        String url = "http://localhost:" + server.getAddress().getPort() + "/repo.zip";

        Path itemsDir = tempDir.resolve("downloaded-empty");
        Files.createDirectories(itemsDir);

        try {
            RuntimeException thrown = assertThrows(RuntimeException.class, () -> ReflectionTestUtils.invokeMethod(
                    client, "downloadAndExtractItems", url, "main", itemsDir));
            assertInstanceOf(IOException.class, thrown.getCause());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void refreshItemsIfStaleDownloadsAndUpdatesHash() throws Exception {
        byte[] zipBytes = buildZip(Map.of(
                "repo/items/ITEM_B.json", "{\"id\":\"ITEM_B\"}"
        ));
        HttpServer server = startZipServer(zipBytes);
        String url = "http://localhost:" + server.getAddress().getPort() + "/repo.zip";

        Path itemsDir = tempDir.resolve("refresh-items");
        Files.createDirectories(itemsDir.resolve("nested"));
        Files.writeString(itemsDir.resolve("nested").resolve("old.json"), "{\"id\":\"old\"}");

        NEUClient localClient = new NEUClient(url, itemsDir.toString(), "main", 1, new NEUItemFilterHandler());
        DataSourceHashRepository repository = mock(DataSourceHashRepository.class);
        when(repository.findBySourceKey("NEU-ITEMS"))
                .thenReturn(new DataSourceHash(null, "NEU-ITEMS", "old-hash", Instant.now().minus(Duration.ofDays(10))));
        ReflectionTestUtils.setField(localClient, "dataSourceHashRepository", repository);

        try {
            boolean updated = localClient.refreshItemsIfStale();
            assertTrue(updated);
        } finally {
            server.stop(0);
        }

        verify(repository).save(any(DataSourceHash.class));
        assertTrue(Files.exists(itemsDir.resolve("ITEM_B.json")));
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
                tempDir.toString(), "main", 0, new NEUItemFilterHandler());
        Boolean due = ReflectionTestUtils.invokeMethod(zeroRefreshClient, "isRefreshDue",
                Instant.now(), Instant.now());

        assertNotNull(due);
        assertTrue(due);
    }

    private Path createItemsDir() throws IOException {
        Path itemsDir = tempDir.resolve("items");
        Path nested = itemsDir.resolve("nested");
        Files.createDirectories(nested);
        copyTestJson("test_jsons/ARMOR_OF_YOG_BOOTS.json", nested.resolve("ARMOR_OF_YOG_BOOTS.json"));
        copyTestJson("test_jsons/ARMADILLO;5.json", itemsDir.resolve("ARMADILLO;5.json"));
        return itemsDir;
    }

    private void copyTestJson(String resourcePath, Path destination) throws IOException {
        try (var input = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (input == null) {
                throw new IOException("Missing test resource: " + resourcePath);
            }
            Files.copy(input, destination);
        }
    }

    private String getString(tools.jackson.databind.JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return "";
        }
        String value = node.asString();
        return value == null ? "" : value;
    }

    private HttpServer startZipServer(byte[] zipBytes) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/repo.zip", exchange -> {
            exchange.sendResponseHeaders(200, zipBytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(zipBytes);
            }
        });
        server.start();
        return server;
    }

    private byte[] buildZip(Map<String, String> entries) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ZipOutputStream zipStream = new ZipOutputStream(output)) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                ZipEntry zipEntry = new ZipEntry(entry.getKey());
                zipStream.putNextEntry(zipEntry);
                zipStream.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                zipStream.closeEntry();
            }
        }
        return output.toByteArray();
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
