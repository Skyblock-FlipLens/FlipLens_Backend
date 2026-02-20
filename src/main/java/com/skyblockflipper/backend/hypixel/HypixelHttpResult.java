package com.skyblockflipper.backend.hypixel;

import org.springframework.http.HttpHeaders;

public record HypixelHttpResult<T>(
        int statusCode,
        HttpHeaders headers,
        T body,
        boolean transportError,
        String errorMessage
) {
    public HypixelHttpResult {
        headers = headers == null ? HttpHeaders.EMPTY : headers;
    }

    public boolean isSuccessful() {
        return statusCode >= 200 && statusCode < 300;
    }

    public static <T> HypixelHttpResult<T> success(int statusCode, HttpHeaders headers, T body) {
        return new HypixelHttpResult<>(statusCode, headers, body, false, null);
    }

    public static <T> HypixelHttpResult<T> error(int statusCode, HttpHeaders headers, String message) {
        return new HypixelHttpResult<>(statusCode, headers, null, false, message);
    }

    public static <T> HypixelHttpResult<T> transportError(String message) {
        return new HypixelHttpResult<>(0, HttpHeaders.EMPTY, null, true, message);
    }
}
