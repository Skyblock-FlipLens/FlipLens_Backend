package com.skyblockflipper.backend.service.flipping.storage;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlipStoragePropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfiguration.class);

    @Test
    void defaultsKeepOptionalHistoryDisabled() {
        contextRunner.run(context -> {
            FlipStorageProperties properties = context.getBean(FlipStorageProperties.class);

            assertTrue(properties.isDualWriteEnabled());
            assertFalse(properties.isReadFromNew());
            assertTrue(properties.isLegacyWriteEnabled());
            assertFalse(properties.isTopSnapshotMaterializationEnabled());
            assertFalse(properties.isSnapshotItemStateCaptureEnabled());
            assertEquals(0.05D, properties.getTrendRelativeThreshold());
            assertEquals(3.0D, properties.getTrendScoreDeltaThreshold());
            assertEquals(20, properties.getParitySampleSize());
        });
    }

    @Test
    void bindsOptionalHistoryTogglesWhenConfigured() {
        contextRunner.withPropertyValues(
                "config.flip.storage.top-snapshot-materialization-enabled=true",
                "config.flip.storage.snapshot-item-state-capture-enabled=true"
        ).run(context -> {
            FlipStorageProperties properties = context.getBean(FlipStorageProperties.class);

            assertTrue(properties.isTopSnapshotMaterializationEnabled());
            assertTrue(properties.isSnapshotItemStateCaptureEnabled());
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(FlipStorageProperties.class)
    static class TestConfiguration {
    }
}
