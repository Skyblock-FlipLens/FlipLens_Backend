package com.skyblockflipper.backend.NEU;

import com.skyblockflipper.backend.model.DataSourceHash;
import com.skyblockflipper.backend.repository.DataSourceHashRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.stream.Stream;


@Slf4j
@Service
public class NEUClient {
    private static final String NEU_SOURCE_KEY = "NEU-ITEMS";
    private final Path itemsDir;
    private final String repoUrl;
    private final String branch;
    private final long refreshDays;
    private final NEUItemFilterHandler itemFilterHandler;
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    private DataSourceHashRepository dataSourceHashRepository;

    public NEUClient(@Value("${config.NEU.repo-url}") String repoUrl,
                     @Value("${config.NEU.items-dir:NotEnoughUpdates-REPO/items}") String itemsDirValue,
                     @Value("${config.NEU.branch:master}") String branch,
                     @Value("${config.NEU.refresh-days:1}") long refreshDays,
                     NEUItemFilterHandler itemFilterHandler) {
        Path resolvedItemsDir = Paths.get(itemsDirValue);
        if (!resolvedItemsDir.isAbsolute()) {
            resolvedItemsDir = Paths.get(System.getProperty("user.dir")).resolve(resolvedItemsDir).normalize();
        }
        this.itemsDir = resolvedItemsDir;
        this.repoUrl = repoUrl;
        this.branch = branch;
        this.refreshDays = refreshDays;
        this.itemFilterHandler = itemFilterHandler;
        log.info("NEU items dir: {}", itemsDir.toAbsolutePath());
    }

    public synchronized boolean updateHash() throws IOException, InterruptedException {
        return refreshItemsIfStale();
    }

    public synchronized void fetchData() throws IOException, InterruptedException {
        if (Files.exists(itemsDir) && hasItemFiles(itemsDir)) {
            log.info("NEU items dir already exists, skipping download.");
            return;
        }
        Files.createDirectories(itemsDir);
        downloadAndExtractItems(repoUrl, branch, itemsDir);
    }

    public synchronized boolean refreshItemsIfStale() throws IOException, InterruptedException {
        DataSourceHash existing = dataSourceHashRepository.findBySourceKey(NEU_SOURCE_KEY);
        boolean hasItems = Files.exists(itemsDir) && hasItemFiles(itemsDir);
        if (existing == null && hasItems) {
            DataSourceHash newHash = computeItemsHash(itemsDir);
            dataSourceHashRepository.save(newHash);
            return true;
        }
        if (existing != null && hasItems && !isRefreshDue(existing.getUpdatedAt(), Instant.now())) {
            return false;
        }
        if (Files.exists(itemsDir)) {
            deleteDirectory(itemsDir);
        }
        fetchData();
        DataSourceHash newHash = computeItemsHash(itemsDir);
        if (existing == null) {
            dataSourceHashRepository.save(newHash);
        } else {
            existing.setHash(newHash.getHash());
            existing.setUpdatedAt(newHash.getUpdatedAt());
            dataSourceHashRepository.save(existing);
        }
        return true;
    }

    private boolean isRefreshDue(Instant lastUpdated, Instant now) {
        if (refreshDays <= 0) {
            return true;
        }
        return now.isAfter(lastUpdated.plus(Duration.ofDays(refreshDays)));
    }

    private void deleteDirectory(Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
    }


    public synchronized List<JsonNode> loadItemJsons() throws IOException, InterruptedException {
        refreshItemsIfStale();
        List<Path> itemPaths;
        try (Stream<Path> paths = Files.walk(itemsDir)) {
            itemPaths = paths.filter(path -> Files.isRegularFile(path)
                            && path.getFileName().toString().endsWith(".json"))
                    .sorted(Comparator.comparing(Path::toString))
                    .toList();
        }

        List<JsonNode> items = new ArrayList<>(itemPaths.size());
        for (Path path : itemPaths) {
            try (InputStream input = Files.newInputStream(path)) {
                items.add(objectMapper.readTree(input));
            }
        }
        return itemFilterHandler.filter(items);
    }

    private DataSourceHash computeItemsHash(Path dir) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }

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
                            try (InputStream input = Files.newInputStream(filePath)) {
                                byte[] buffer = new byte[8192];
                                int read;
                                while ((read = input.read(buffer)) != -1) {
                                    digest.update(buffer, 0, read);
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
        return new DataSourceHash(NEU_SOURCE_KEY, toHex(digest.digest()));
    }

    private String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private boolean hasItemFiles(Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            return paths.anyMatch(path -> Files.isRegularFile(path)
                    && path.getFileName().toString().endsWith(".json"));
        }
    }

    private void downloadAndExtractItems(String repoUrl, String branch, Path targetDir) throws IOException, InterruptedException {
        String zipUrl = resolveZipUrl(repoUrl, branch);
        Path tempZip = null;
        try {
            tempZip = Files.createTempFile("neu-repo-", ".zip");
            HttpClient client = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(zipUrl))
                    .GET()
                    .build();
            HttpResponse<Path> response = client.send(request, HttpResponse.BodyHandlers.ofFile(tempZip));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IOException("Failed to download NEU repo zip: HTTP " + response.statusCode());
            }

            Path normalizedTargetDir = targetDir.toAbsolutePath().normalize();
            int extracted = 0;
            try (InputStream fileStream = Files.newInputStream(tempZip);
                 ZipInputStream zipStream = new ZipInputStream(fileStream)) {
                ZipEntry entry;
                while ((entry = zipStream.getNextEntry()) != null) {
                    if (entry.isDirectory()) {
                        continue;
                    }
                    String name = entry.getName();
                    int itemsIndex = name.indexOf("/items/");
                    if (itemsIndex < 0 || !name.endsWith(".json")) {
                        continue;
                    }
                    String relative = name.substring(itemsIndex + "/items/".length());
                    Path outPath = targetDir.resolve(relative).toAbsolutePath().normalize();
                    if (!outPath.startsWith(normalizedTargetDir)) {
                        throw new IOException("Bad zip entry (Zip Slip attempt): " + name);
                    }
                    Files.createDirectories(outPath.getParent());
                    Files.copy(zipStream, outPath, StandardCopyOption.REPLACE_EXISTING);
                    extracted++;
                }
            }

            if (extracted == 0) {
                throw new IOException("No item JSONs found in NEU repo zip.");
            }
        } finally {
            Files.deleteIfExists(tempZip);
        }
    }

    private String resolveZipUrl(String repoUrl, String branch) {
        if (repoUrl.endsWith(".zip")) {
            return repoUrl;
        }
        if (repoUrl.endsWith("/")) {
            repoUrl = repoUrl.substring(0, repoUrl.length() - 1);
        }
        if (repoUrl.contains("github.com")) {
            URI uri = URI.create(repoUrl);
            String[] parts = uri.getPath().split("/");
            if (parts.length >= 3) {
                String owner = parts[1];
                String repo = parts[2];
                return "https://codeload.github.com/" + owner + "/" + repo + "/zip/refs/heads/" + branch;
            }
        }
        throw new IllegalArgumentException("Unsupported NEU repo URL, provide a direct .zip URL: " + repoUrl);
    }

}
