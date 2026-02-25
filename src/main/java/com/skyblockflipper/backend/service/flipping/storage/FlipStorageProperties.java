package com.skyblockflipper.backend.service.flipping.storage;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "config.flip.storage")
public class FlipStorageProperties {

    private boolean dualWriteEnabled = true;
    private boolean readFromNew = false;
    private boolean legacyWriteEnabled = true;
    private boolean topSnapshotMaterializationEnabled = false;
    private boolean snapshotItemStateCaptureEnabled = false;
    private double trendRelativeThreshold = 0.05D;
    private double trendScoreDeltaThreshold = 3.0D;
    private int paritySampleSize = 20;

}
