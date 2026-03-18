package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.instrumentation.InstrumentationProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApplicationLogFileServiceTest {

    @Test
    void readTailReturnsLastLinesFromConfiguredFile() throws Exception {
        Path logFile = Files.createTempFile("application-log-service", ".log");
        Files.writeString(logFile, String.join(System.lineSeparator(),
                IntStream.rangeClosed(1, 10).mapToObj(i -> "line-" + i).toList()));

        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getLogs().setFile(logFile);
        properties.getLogs().setMaxLines(5);
        properties.getLogs().setMaxReadBytes(256);

        ApplicationLogFileService service = new ApplicationLogFileService(properties, new MockEnvironment());
        ApplicationLogFileService.ApplicationLogTail tail = service.readTail(3);

        assertTrue(tail.available());
        assertEquals(logFile, tail.file());
        assertEquals("line-8" + System.lineSeparator() + "line-9" + System.lineSeparator() + "line-10", tail.content());
    }

    @Test
    void resolveLogFileFallsBackToSpringLoggingProperties() {
        InstrumentationProperties properties = new InstrumentationProperties();
        MockEnvironment environment = new MockEnvironment()
                .withProperty("logging.file.path", "var/logs");
        ApplicationLogFileService service = new ApplicationLogFileService(properties, environment);

        Path resolved = service.resolveLogFile();

        assertEquals(Path.of("var/logs").resolve("spring.log"), resolved);
    }
}
