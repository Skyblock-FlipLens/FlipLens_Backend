package com.skyblockflipper.backend.api;

public enum FlipSortBy {
    EXPECTED_PROFIT("expectedProfit"),
    ROI("roi"),
    ROI_PER_HOUR("roiPerHour"),
    LIQUIDITY_SCORE("liquidityScore"),
    RISK_SCORE("riskScore"),
    REQUIRED_CAPITAL("requiredCapital"),
    FEES("fees"),
    DURATION_SECONDS("durationSeconds");

    private final String fieldName;

    FlipSortBy(String fieldName) {
        this.fieldName = fieldName;
    }

    public String toFieldName() {
        return fieldName;
    }
}
