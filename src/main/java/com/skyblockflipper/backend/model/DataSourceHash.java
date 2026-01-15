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
@Entity
@Table(name = "data_source_hash")
public class DataSourceHash {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Setter
    @Column(nullable = false, unique = true, length = 120)
    private String sourceKey;

    @Setter
    @Column(nullable = false, length = 128)
    private String hash;

    @Setter
    @Column(nullable = false)
    private Instant updatedAt;

    protected DataSourceHash() {
    }

    public DataSourceHash(UUID id, String sourceKey, String hash, Instant updatedAt) {
        this.id = id;
        this.sourceKey = sourceKey;
        this.hash = hash;
        this.updatedAt = updatedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataSourceHash that = (DataSourceHash) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
