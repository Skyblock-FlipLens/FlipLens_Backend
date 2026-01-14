package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class StatusController {
	private final HypixelClient hypixelClient;

	@GetMapping("/status")
	public StatusResponse status() {
		hypixelClient.fetchAuctions();
		return new StatusResponse("ok");
	}
}
