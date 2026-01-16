package com.skyblockflipper.backend.NEU;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.stream.Stream;


@Slf4j
@Service
public class NEUClient {
    private final Path itemsDir;

    public NEUClient(@Value("${config.NEU.repo-url}") String repoUrl,
                     @Value("${config.NEU.items-dir:NotEnoughUpdates-REPO/items}") String itemsDirValue,
                     @Value("${config.NEU.branch:master}") String branch) throws IOException, InterruptedException {
        Path resolvedItemsDir = Paths.get(itemsDirValue);
        if (!resolvedItemsDir.isAbsolute()) {
            resolvedItemsDir = Paths.get(System.getProperty("user.dir")).resolve(resolvedItemsDir).normalize();
        }
        this.itemsDir = resolvedItemsDir;
        log.info("NEU items dir: {}", itemsDir.toAbsolutePath());

        if (Files.exists(itemsDir) && hasItemFiles(itemsDir)) {
            log.info("NEU items dir already exists, skipping download.");
            return;
        }

        Files.createDirectories(itemsDir);
        downloadAndExtractItems(repoUrl, branch, itemsDir);
    }

    private boolean hasItemFiles(Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            return paths.anyMatch(path -> Files.isRegularFile(path)
                    && path.getFileName().toString().endsWith(".json"));
        }
    }

    private void downloadAndExtractItems(String repoUrl, String branch, Path targetDir) throws IOException, InterruptedException {
        String zipUrl = resolveZipUrl(repoUrl, branch);
        Path tempZip = Files.createTempFile("neu-repo-", ".zip");
        try {
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
                    Path outPath = targetDir.resolve(relative);
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
