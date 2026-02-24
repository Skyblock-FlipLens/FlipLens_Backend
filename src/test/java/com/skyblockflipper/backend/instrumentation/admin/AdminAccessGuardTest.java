package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.instrumentation.InstrumentationProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AdminAccessGuardTest {

    @Test
    void validateAllowsLoopbackWithoutToken() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(true);
        properties.getAdmin().setToken("");
        AdminAccessGuard guard = new AdminAccessGuard(properties);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");

        assertDoesNotThrow(() -> guard.validate(request));
    }

    @Test
    void validateAllowsIpv6LoopbackWithoutToken() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(true);
        properties.getAdmin().setToken("");

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("::1");

        assertDoesNotThrow(() -> new AdminAccessGuard(properties).validate(request));
    }

    @Test
    void validateRejectsNonLoopbackWhenLocalOnlyEnabled() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(true);
        properties.getAdmin().setToken("");
        AdminAccessGuard guard = new AdminAccessGuard(properties);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("8.8.8.8");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> guard.validate(request));
        assertEquals(403, exception.getStatusCode().value());
    }

    @Test
    void validateAllowsRemoteAddressWhenWithinConfiguredSubnet() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(true);
        properties.getAdmin().setToken("");
        properties.getAdmin().setAllowedSubnets(List.of("192.168.10.0/24"));
        AdminAccessGuard guard = new AdminAccessGuard(properties);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("192.168.10.99");

        assertDoesNotThrow(() -> guard.validate(request));
    }

    @Test
    void validateRejectsRemoteAddressOutsideConfiguredSubnet() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(true);
        properties.getAdmin().setToken("");
        properties.getAdmin().setAllowedSubnets(List.of("192.168.10.0/24"));
        AdminAccessGuard guard = new AdminAccessGuard(properties);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("192.168.11.10");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> guard.validate(request));
        assertEquals(403, exception.getStatusCode().value());
    }

    @Test
    void validateRejectsRemoteAddressWhenOnlyInvalidSubnetConfigured() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(true);
        properties.getAdmin().setToken("");
        properties.getAdmin().setAllowedSubnets(List.of("192.168.10.0/not-a-prefix"));
        AdminAccessGuard guard = new AdminAccessGuard(properties);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("192.168.10.99");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> guard.validate(request));
        assertEquals(403, exception.getStatusCode().value());
    }

    @Test
    void validateStillAllowsLoopbackWhenSubnetConfigurationInvalid() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(true);
        properties.getAdmin().setToken("");
        properties.getAdmin().setAllowedSubnets(List.of("bad-cidr"));
        AdminAccessGuard guard = new AdminAccessGuard(properties);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");

        assertDoesNotThrow(() -> guard.validate(request));
    }

    @Test
    void validateRequiresMatchingAdminTokenWhenConfigured() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(false);
        properties.getAdmin().setToken("secret");
        AdminAccessGuard guard = new AdminAccessGuard(properties);

        MockHttpServletRequest invalid = new MockHttpServletRequest();
        invalid.setRemoteAddr("127.0.0.1");
        invalid.addHeader("X-Admin-Token", "wrong");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> guard.validate(invalid));
        assertEquals(401, exception.getStatusCode().value());

        MockHttpServletRequest valid = new MockHttpServletRequest();
        valid.setRemoteAddr("127.0.0.1");
        valid.addHeader("X-Admin-Token", "secret");
        assertDoesNotThrow(() -> guard.validate(valid));
    }

    @Test
    void validateReturnsUnauthorizedWhenTokenHeaderMissing() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(false);
        properties.getAdmin().setToken("secret");
        AdminAccessGuard guard = new AdminAccessGuard(properties);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> guard.validate(request));
        assertEquals(401, exception.getStatusCode().value());
    }

    @Test
    void validateStillRequiresTokenWhenLocalOnlyAndSubnetAllowed() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getAdmin().setLocalOnly(true);
        properties.getAdmin().setToken("secret");
        properties.getAdmin().setAllowedSubnets(List.of("192.168.10.0/24"));
        AdminAccessGuard guard = new AdminAccessGuard(properties);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("192.168.10.20");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> guard.validate(request));
        assertEquals(401, exception.getStatusCode().value());
    }
}
