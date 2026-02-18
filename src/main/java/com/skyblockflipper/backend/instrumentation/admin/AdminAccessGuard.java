package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.instrumentation.InstrumentationProperties;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

@Component
@RequiredArgsConstructor
public class AdminAccessGuard {

    private final InstrumentationProperties properties;


    public void validate(HttpServletRequest request) {
        if (properties.getAdmin().isLocalOnly() && !isLoopback(request.getRemoteAddr())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Admin endpoint is local-only");
        }
        String configuredToken = properties.getAdmin().getToken();
        if (configuredToken != null && !configuredToken.isBlank()) {
            String provided = request.getHeader("X-Admin-Token");
            byte[] configuredTokenBytes = configuredToken.getBytes(StandardCharsets.UTF_8);
            byte[] providedBytes = provided == null ? new byte[0] : provided.getBytes(StandardCharsets.UTF_8);
            if (!MessageDigest.isEqual(configuredTokenBytes, providedBytes)) {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Missing or invalid admin token");
            }
        }
    }

    private boolean isLoopback(String remoteAddr) {
        try {
            return InetAddress.getByName(remoteAddr).isLoopbackAddress();
        } catch (Exception ignored) {
            return false;
        }
    }
}
