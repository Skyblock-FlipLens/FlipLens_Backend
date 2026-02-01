package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.NEU.model.Item;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ItemRepository extends JpaRepository<Item, String> {
}
