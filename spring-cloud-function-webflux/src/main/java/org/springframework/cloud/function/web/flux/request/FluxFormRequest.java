package org.springframework.cloud.function.web.flux.request;

import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import reactor.core.publisher.Flux;

public class FluxFormRequest {

	private MultiValueMap<String, String> map;

	public FluxFormRequest(MultiValueMap<String, String> map) {
		this.map = new LinkedMultiValueMap<>(map);
	}

	public static FluxFormRequest from(MultiValueMap<String, String> map) {
		return new FluxFormRequest(map);
	}

	public Flux<MultiValueMap<String, String>> flux() {
		return Flux.just(map);
	}

	public MultiValueMap<String, String> body() {
		return map;
	}

}
