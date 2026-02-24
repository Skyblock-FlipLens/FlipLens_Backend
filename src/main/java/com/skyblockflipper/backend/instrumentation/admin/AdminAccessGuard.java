package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.instrumentation.InstrumentationProperties;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class AdminAccessGuard {

    private final InstrumentationProperties properties;


    public void validate(HttpServletRequest request) {
        if (properties.getAdmin().isLocalOnly()) {
            String remoteAddr = request.getRemoteAddr();
            if (!isLoopback(remoteAddr) && !isAllowedBySubnet(remoteAddr)) {
                throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Admin endpoint is local-only");
            }
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

    private boolean isAllowedBySubnet(String remoteAddr) {
        List<String> allowedSubnets = properties.getAdmin().getAllowedSubnets();
        if (allowedSubnets == null || allowedSubnets.isEmpty()) {
            return false;
        }

        InetAddress remoteAddress = toInetAddress(remoteAddr);
        if (remoteAddress == null) {
            return false;
        }

        for (String cidr : allowedSubnets) {
            if (cidr == null || cidr.isBlank()) {
                continue;
            }
            if (isInSubnet(remoteAddress, cidr.trim())) {
                return true;
            }
        }
        return false;
    }

    private InetAddress toInetAddress(String address) {
        try {
            return InetAddress.getByName(address);
        } catch (Exception exception) {
            log.warn("Unable to parse remote admin address '{}': {}", address, exception.getMessage());
            return null;
        }
    }

    private boolean isInSubnet(InetAddress remoteAddress, String cidr) {
        String[] parts = cidr.split("/");
        if (parts.length != 2) {
            log.warn("Ignoring invalid instrumentation.admin.allowed-subnets entry '{}'", cidr);
            return false;
        }
        try {
            InetAddress networkAddress = InetAddress.getByName(parts[0]);
            int prefixLength = Integer.parseInt(parts[1]);
            byte[] remoteBytes = remoteAddress.getAddress();
            byte[] networkBytes = networkAddress.getAddress();
            if (remoteBytes.length != networkBytes.length) {
                return false;
            }
            int bitLength = remoteBytes.length * 8;
            if (prefixLength < 0 || prefixLength > bitLength) {
                log.warn("Ignoring invalid CIDR prefix in instrumentation.admin.allowed-subnets entry '{}'", cidr);
                return false;
            }
            int fullBytes = prefixLength / 8;
            int remainderBits = prefixLength % 8;
            for (int i = 0; i < fullBytes; i++) {
                if (remoteBytes[i] != networkBytes[i]) {
                    return false;
                }
            }
            if (remainderBits == 0) {
                return true;
            }
            int mask = 0xFF << (8 - remainderBits);
            return (remoteBytes[fullBytes] & mask) == (networkBytes[fullBytes] & mask);
        } catch (NumberFormatException exception) {
            log.warn("Ignoring invalid CIDR prefix in instrumentation.admin.allowed-subnets entry '{}'", cidr);
            return false;
        } catch (Exception exception) {
            log.warn("Ignoring invalid instrumentation.admin.allowed-subnets entry '{}': {}", cidr, exception.getMessage());
            return false;
        }
    }
}
