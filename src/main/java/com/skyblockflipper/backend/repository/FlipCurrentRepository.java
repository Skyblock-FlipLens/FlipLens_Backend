package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.flippingstorage.FlipCurrentEntity;
import com.skyblockflipper.backend.model.flippingstorage.FlipDefinitionEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface FlipCurrentRepository extends JpaRepository<FlipCurrentEntity, String> {

    List<FlipCurrentEntity> findAllByFlipType(FlipType flipType);

    Page<FlipCurrentEntity> findAllByFlipType(FlipType flipType, Pageable pageable);

    Optional<FlipCurrentEntity> findByStableFlipId(UUID stableFlipId);

    @Query("""
            select fc as current, fd as definition
            from FlipCurrentEntity fc
            join FlipDefinitionEntity fd on fd.flipKey = fc.flipKey
            """)
    List<CurrentDefinitionProjection> findAllWithDefinition();

    @Query("""
            select fc as current, fd as definition
            from FlipCurrentEntity fc
            join FlipDefinitionEntity fd on fd.flipKey = fc.flipKey
            where fc.flipType = :flipType
            """)
    List<CurrentDefinitionProjection> findAllWithDefinitionByFlipType(@Param("flipType") FlipType flipType);

    @Query(value = """
            select fc as current, fd as definition
            from FlipCurrentEntity fc
            join FlipDefinitionEntity fd on fd.flipKey = fc.flipKey
            """,
            countQuery = "select count(fc) from FlipCurrentEntity fc")
    Page<CurrentDefinitionProjection> findAllWithDefinition(Pageable pageable);

    @Query(value = """
            select fc as current, fd as definition
            from FlipCurrentEntity fc
            join FlipDefinitionEntity fd on fd.flipKey = fc.flipKey
            where fc.flipType = :flipType
            """,
            countQuery = "select count(fc) from FlipCurrentEntity fc where fc.flipType = :flipType")
    Page<CurrentDefinitionProjection> findAllWithDefinitionByFlipType(@Param("flipType") FlipType flipType,
                                                                      Pageable pageable);

    @Query("""
            select fc as current, fd as definition
            from FlipCurrentEntity fc
            join FlipDefinitionEntity fd on fd.flipKey = fc.flipKey
            where fc.stableFlipId = :stableFlipId
            """)
    Optional<CurrentDefinitionProjection> findByStableFlipIdWithDefinition(@Param("stableFlipId") UUID stableFlipId);

    @Query("""
            select fc as current, fd as definition
            from FlipCurrentEntity fc
            join FlipDefinitionEntity fd on fd.flipKey = fc.flipKey
            where fc.stableFlipId in :stableFlipIds
            """)
    List<CurrentDefinitionProjection> findAllWithDefinitionByStableFlipIds(
            @Param("stableFlipIds") List<UUID> stableFlipIds
    );

    @Query(value = """
            select fc as current, fd as definition
            from FlipCurrentEntity fc
            join FlipDefinitionEntity fd on fd.flipKey = fc.flipKey
            where (:flipType is null or fc.flipType = :flipType)
              and (:minLiquidityScore is null or fc.liquidityScore >= :minLiquidityScore)
              and (:maxRiskScore is null or fc.riskScore <= :maxRiskScore)
              and (:minExpectedProfit is null or fc.expectedProfit >= :minExpectedProfit)
              and (:minRoi is null or fc.roi >= :minRoi)
              and (:minRoiPerHour is null or fc.roiPerHour >= :minRoiPerHour)
              and (:maxRequiredCapital is null or fc.requiredCapital <= :maxRequiredCapital)
              and (:partial is null or fc.partial = :partial)
              and fc.partialReasonsJson not like '%MISSING_INPUT_PRICE%'
              and fc.partialReasonsJson not like '%INSUFFICIENT_INPUT_DEPTH%'
            """,
            countQuery = """
            select count(fc)
            from FlipCurrentEntity fc
            where (:flipType is null or fc.flipType = :flipType)
              and (:minLiquidityScore is null or fc.liquidityScore >= :minLiquidityScore)
              and (:maxRiskScore is null or fc.riskScore <= :maxRiskScore)
              and (:minExpectedProfit is null or fc.expectedProfit >= :minExpectedProfit)
              and (:minRoi is null or fc.roi >= :minRoi)
              and (:minRoiPerHour is null or fc.roiPerHour >= :minRoiPerHour)
              and (:maxRequiredCapital is null or fc.requiredCapital <= :maxRequiredCapital)
              and (:partial is null or fc.partial = :partial)
              and fc.partialReasonsJson not like '%MISSING_INPUT_PRICE%'
              and fc.partialReasonsJson not like '%INSUFFICIENT_INPUT_DEPTH%'
            """)
    Page<CurrentDefinitionProjection> findFilteredWithDefinition(
            @Param("flipType") FlipType flipType,
            @Param("minLiquidityScore") Double minLiquidityScore,
            @Param("maxRiskScore") Double maxRiskScore,
            @Param("minExpectedProfit") Long minExpectedProfit,
            @Param("minRoi") Double minRoi,
            @Param("minRoiPerHour") Double minRoiPerHour,
            @Param("maxRequiredCapital") Long maxRequiredCapital,
            @Param("partial") Boolean partial,
            Pageable pageable
    );

    boolean existsBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis);

    void deleteBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis);

    @Query("select max(fc.snapshotTimestampEpochMillis) from FlipCurrentEntity fc")
    Optional<Long> findMaxSnapshotTimestampEpochMillis();

    @Query("select fc.flipType as flipType, count(fc) as count from FlipCurrentEntity fc group by fc.flipType")
    List<FlipTypeCountProjection> countByFlipType();

    interface FlipTypeCountProjection {
        FlipType getFlipType();
        long getCount();
    }

    interface CurrentDefinitionProjection {
        FlipCurrentEntity getCurrent();
        FlipDefinitionEntity getDefinition();
    }
}
