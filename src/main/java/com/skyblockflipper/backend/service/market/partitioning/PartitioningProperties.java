package com.skyblockflipper.backend.service.market.partitioning;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "config.snapshot.partitioning")
public class PartitioningProperties {

    private boolean enabled = false;
    private PartitioningMode mode = PartitioningMode.ROW_DELETE;
    private boolean dryRun = true;
    private boolean fallbackToRowDelete = true;
    private String schemaName = "public";
    private int precreateDays = 14;

    private String marketSnapshotParentTable = "market_snapshot";
    private int marketSnapshotRetentionDays = 7;

    private String ahSnapshotParentTable = "ah_item_snapshot";
    private int ahSnapshotRetentionDays = 30;

    private String bzSnapshotParentTable = "bz_item_snapshot";
    private int bzSnapshotRetentionDays = 30;
}
