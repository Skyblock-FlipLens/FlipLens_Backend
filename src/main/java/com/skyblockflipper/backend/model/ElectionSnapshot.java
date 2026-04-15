package com.skyblockflipper.backend.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "election_snapshot")
public class ElectionSnapshot {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private Instant fetchedAt;

    @Column(nullable = false, length = 128)
    private String payloadHash;

    @Column(nullable = false, columnDefinition = "text")
    private String payloadJson;

    protected ElectionSnapshot() {
    }

    public ElectionSnapshot(UUID id, Instant fetchedAt, String payloadHash, String payloadJson) {
        this.id = id;
        this.fetchedAt = fetchedAt;
        this.payloadHash = payloadHash;
        this.payloadJson = payloadJson;
    }

    public ElectionSnapshot(Instant fetchedAt, String payloadHash, String payloadJson) {
        this.fetchedAt = fetchedAt;
        this.payloadHash = payloadHash;
        this.payloadJson = payloadJson;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElectionSnapshot that = (ElectionSnapshot) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
