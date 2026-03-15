package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.ItemBucketMaterializationStateEntity;
import com.skyblockflipper.backend.model.market.ItemBucketMaterializationStateId;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ItemBucketMaterializationStateRepository extends JpaRepository<ItemBucketMaterializationStateEntity, ItemBucketMaterializationStateId> {

    ItemBucketMaterializationStateEntity findByBucketStartEpochMillisAndBucketGranularityAndMarketType(long bucketStartEpochMillis,
                                                                                                        String bucketGranularity,
                                                                                                        String marketType);

    ItemBucketMaterializationStateEntity findTopByMarketTypeAndBucketGranularityAndFinalizedTrueOrderByBucketStartEpochMillisDesc(String marketType,
                                                                                                                                   String bucketGranularity);

    ItemBucketMaterializationStateEntity findTopByMarketTypeAndBucketGranularityOrderByBucketStartEpochMillisDesc(String marketType,
                                                                                                                   String bucketGranularity);

    long countBySourcePartitionAndMarketTypeAndBucketGranularityAndFinalizedTrue(String sourcePartition,
                                                                                 String marketType,
                                                                                 String bucketGranularity);

    long countBySourcePartitionAndMarketTypeAndBucketGranularityAndFailedTrue(String sourcePartition,
                                                                              String marketType,
                                                                              String bucketGranularity);
}
