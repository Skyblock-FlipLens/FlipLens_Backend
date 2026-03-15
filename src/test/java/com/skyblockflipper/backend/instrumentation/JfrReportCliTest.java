package com.skyblockflipper.backend.instrumentation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JfrReportCliTest {

    @Test
    @ResourceLock(Resources.SYSTEM_OUT)
    void mainPrintsErrorForMissingRecordingPath() {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String missingPath = "missing-recording-" + UUID.randomUUID() + ".jfr";
        try {
            System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));

            JfrReportCli.main(new String[]{missingPath});

            String printed = out.toString(StandardCharsets.UTF_8);
            assertTrue(printed.contains("Failed to parse JFR"));
        } finally {
            System.setOut(originalOut);
        }
    }
}
