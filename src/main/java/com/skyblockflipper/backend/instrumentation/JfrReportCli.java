package com.skyblockflipper.backend.instrumentation;

import java.nio.file.Path;

public final class JfrReportCli {

    private JfrReportCli() {
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: JfrReportCli <recording.jfr>");
            System.exit(1);
        }
        JfrBlockingReportService service = new JfrBlockingReportService(new InstrumentationProperties());
        System.out.println(service.summarize(Path.of(args[0])));
    }
}
