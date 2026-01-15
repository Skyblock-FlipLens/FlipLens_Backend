package com.skyblockflipper.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EntityScan("com.skyblockflipper.backend.NEU")
public class SkyblockFlipperBackendApplication {

	public static void main(String[] args) {
		SpringApplication.run(SkyblockFlipperBackendApplication.class, args);
	}
}
