package com.skyblockflipper.backend.instrumentation;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JfrReportCliTest {

    @Test
    void mainPrintsSummaryForProvidedRecordingPath() {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));

            JfrReportCli.main(new String[]{"missing-recording.jfr"});

            String printed = out.toString(StandardCharsets.UTF_8);
            assertTrue(printed.contains("Failed to parse JFR"));
        } finally {
            System.setOut(originalOut);
        }
    }
}
