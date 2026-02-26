package com.skyblockflipper.backend.repository;

import java.util.UUID;

public interface MarketSnapshotCompactionCandidate {

    UUID getId();

    long getSnapshotTimestampEpochMillis();
}
