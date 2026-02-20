package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.hypixel.HypixelHttpResult;
import org.springframework.http.HttpHeaders;
import org.springframework.util.StringUtils;

public class ChangeDetector {

    private String lastEtag;
    private String lastModified;
    private String lastHash;

    public synchronized ConditionalHeaders conditionalHeaders() {
        return new ConditionalHeaders(lastEtag, lastModified);
    }

    public synchronized ChangeDecision evaluate(HypixelHttpResult<?> result, String fallbackHash) {
        int status = result.statusCode();
        if (status == 304) {
            return ChangeDecision.noChange();
        }
        if (status == 429) {
            return ChangeDecision.rateLimited();
        }
        if (!result.isSuccessful()) {
            return ChangeDecision.error();
        }

        HttpHeaders headers = result.headers();
        String etag = headers.getETag();
        String modified = headers.getFirst(HttpHeaders.LAST_MODIFIED);

        boolean changed = false;
        if (StringUtils.hasText(etag)) {
            changed = !etag.equals(lastEtag);
            lastEtag = etag;
        } else if (StringUtils.hasText(modified)) {
            changed = !modified.equals(lastModified);
            lastModified = modified;
        }

        if (!changed && StringUtils.hasText(fallbackHash)) {
            changed = !fallbackHash.equals(lastHash);
        }
        if (StringUtils.hasText(fallbackHash)) {
            lastHash = fallbackHash;
        }

        return changed ? ChangeDecision.changed() : ChangeDecision.noChange();
    }

    public record ConditionalHeaders(String ifNoneMatch, String ifModifiedSince) {
    }

    public record ChangeDecision(Decision decision) {
        public static ChangeDecision changed() {
            return new ChangeDecision(Decision.CHANGED);
        }

        public static ChangeDecision noChange() {
            return new ChangeDecision(Decision.NO_CHANGE);
        }

        public static ChangeDecision rateLimited() {
            return new ChangeDecision(Decision.RATE_LIMITED);
        }

        public static ChangeDecision error() {
            return new ChangeDecision(Decision.ERROR);
        }

        public boolean isChanged() {
            return decision == Decision.CHANGED;
        }
    }

    public enum Decision {
        CHANGED,
        NO_CHANGE,
        RATE_LIMITED,
        ERROR
    }
}
