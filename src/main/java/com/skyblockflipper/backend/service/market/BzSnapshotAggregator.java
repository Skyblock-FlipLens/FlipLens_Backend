package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Service
public class BzSnapshotAggregator {

    private final MarketItemKeyService marketItemKeyService;

    public BzSnapshotAggregator() {
        this(new MarketItemKeyService());
    }

    @Autowired
    public BzSnapshotAggregator(MarketItemKeyService marketItemKeyService) {
        this.marketItemKeyService = marketItemKeyService;
    }

    public List<BzItemSnapshotEntity> aggregate(Instant snapshotTimestamp, Map<String, BazaarMarketRecord> bazaarProducts) {
        if (snapshotTimestamp == null || bazaarProducts == null || bazaarProducts.isEmpty()) {
            return List.of();
        }
        long snapshotTs = snapshotTimestamp.toEpochMilli();
        List<BzItemSnapshotEntity> aggregates = new ArrayList<>(bazaarProducts.size());
        for (Map.Entry<String, BazaarMarketRecord> entry : bazaarProducts.entrySet()) {
            BazaarMarketRecord record = entry.getValue();
            if (record == null) {
                continue;
            }
            String productId = marketItemKeyService.toBazaarItemKey(record);
            if (productId == null || productId.isBlank()) {
                productId = marketItemKeyService.normalizeBazaarProductId(entry.getKey());
            }
            if (productId == null || productId.isBlank()) {
                continue;
            }
            aggregates.add(new BzItemSnapshotEntity(
                    snapshotTs,
                    productId,
                    sanitizePrice(record.buyPrice()),
                    sanitizePrice(record.sellPrice()),
                    sanitizeVolume(record.buyVolume()),
                    sanitizeVolume(record.sellVolume())
            ));
        }
        aggregates.sort(Comparator.comparing(BzItemSnapshotEntity::getProductId));
        return aggregates;
    }

    private Double sanitizePrice(double price) {
        if (price <= 0D || Double.isNaN(price) || Double.isInfinite(price)) {
            return null;
        }
        return price;
    }

    private Long sanitizeVolume(long volume) {
        if (volume < 0L) {
            return null;
        }
        return volume;
    }
}
