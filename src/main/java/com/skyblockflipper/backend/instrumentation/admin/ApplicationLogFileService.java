package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.instrumentation.InstrumentationProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ApplicationLogFileService {

    private final InstrumentationProperties properties;
    private final Environment environment;

    public ApplicationLogTail readTail(int requestedLines) {
        if (!properties.getLogs().isEnabled()) {
            return ApplicationLogTail.unavailable("log_endpoint_disabled");
        }

        Path logFile = resolveLogFile();
        if (logFile == null) {
            return ApplicationLogTail.unavailable("log_file_not_configured");
        }
        if (!Files.exists(logFile) || !Files.isRegularFile(logFile)) {
            return ApplicationLogTail.unavailable("log_file_missing", logFile);
        }

        int lineLimit = sanitizeLineLimit(requestedLines);
        long maxReadBytes = sanitizeMaxReadBytes(properties.getLogs().getMaxReadBytes());

        try {
            long sizeBytes = Files.size(logFile);
            long startOffset = Math.max(0L, sizeBytes - maxReadBytes);
            byte[] bytes;
            try (var channel = Files.newByteChannel(logFile, StandardOpenOption.READ)) {
                channel.position(startOffset);
                ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(Integer.MAX_VALUE, sizeBytes - startOffset));
                while (buffer.hasRemaining() && channel.read(buffer) > 0) {
                    // continue reading until EOF or buffer filled
                }
                buffer.flip();
                bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
            }

            String content = new String(bytes, StandardCharsets.UTF_8);
            if (startOffset > 0L) {
                int firstNewline = content.indexOf('\n');
                if (firstNewline >= 0 && firstNewline + 1 < content.length()) {
                    content = content.substring(firstNewline + 1);
                }
            }

            List<String> lines = content.lines().toList();
            int fromIndex = Math.max(0, lines.size() - lineLimit);
            List<String> tailLines = lines.subList(fromIndex, lines.size());
            FileTime lastModified = Files.getLastModifiedTime(logFile);
            return new ApplicationLogTail(
                    true,
                    logFile,
                    sizeBytes,
                    lastModified == null ? null : lastModified.toInstant(),
                    startOffset > 0L,
                    lineLimit,
                    String.join(System.lineSeparator(), tailLines),
                    null
            );
        } catch (Exception exception) {
            return ApplicationLogTail.unavailable("log_read_failed:" + summarize(exception), logFile);
        }
    }

    Path resolveLogFile() {
        Path configured = properties.getLogs().getFile();
        if (configured != null) {
            return configured;
        }

        String loggingFileName = environment.getProperty("logging.file.name");
        if (loggingFileName != null && !loggingFileName.isBlank()) {
            return Path.of(loggingFileName.trim());
        }

        String loggingFilePath = environment.getProperty("logging.file.path");
        if (loggingFilePath != null && !loggingFilePath.isBlank()) {
            return Path.of(loggingFilePath.trim()).resolve("spring.log");
        }

        return null;
    }

    private int sanitizeLineLimit(int requestedLines) {
        int maxLines = Math.max(1, properties.getLogs().getMaxLines());
        return Math.min(maxLines, Math.max(1, requestedLines));
    }

    private long sanitizeMaxReadBytes(long requestedBytes) {
        return Math.max(4_096L, requestedBytes);
    }

    private String summarize(Exception exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return message.length() > 160 ? message.substring(0, 160) : message;
    }

    public record ApplicationLogTail(boolean available,
                                     Path file,
                                     Long sizeBytes,
                                     Instant lastModifiedUtc,
                                     boolean truncatedToRecentBytes,
                                     Integer lineLimit,
                                     String content,
                                     String error) {

        static ApplicationLogTail unavailable(String error) {
            return unavailable(error, null);
        }

        static ApplicationLogTail unavailable(String error, Path file) {
            return new ApplicationLogTail(false, file, null, null, false, null, "", error);
        }
    }
}
