/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.function.web.flux;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;

import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.context.message.MessageUtils;
import org.springframework.cloud.function.json.JsonMapper;
import org.springframework.cloud.function.web.flux.constants.WebRequestConstants;
import org.springframework.cloud.function.web.flux.request.FluxFormRequest;
import org.springframework.cloud.function.web.util.HeaderUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.BodyBuilder;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.ServerWebExchange;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Dave Syer
 * @author Mark Fisher
 */
@Component
public class FunctionController {

	private static Log logger = LogFactory.getLog(FunctionController.class);

	private FunctionInspector inspector;

	private boolean debug = false;

	private StringConverter converter;

	private JsonMapper mapper;

	public FunctionController(JsonMapper mapper, FunctionInspector inspector,
			StringConverter converter) {
		this.mapper = mapper;
		this.inspector = inspector;
		this.converter = converter;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	@PostMapping(path = "/**", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
	@ResponseBody
	public Mono<ResponseEntity<?>> form(ServerWebExchange request) {
		Object function = request.getAttribute(WebRequestConstants.FUNCTION);
		if (function == null) {
			function = request.getAttribute(WebRequestConstants.CONSUMER);
		}
		return post(request, null, request.getFormData().block());
	}

	@PostMapping(path = "/**")
	@ResponseBody
	public Mono<ResponseEntity<?>> post(ServerWebExchange request,
			@RequestBody(required = false) String body) {
		Object function = request.getAttribute(WebRequestConstants.FUNCTION);
		if (function == null) {
			function = request.getAttribute(WebRequestConstants.CONSUMER);
		}
		if (!StringUtils.hasText(body)) {
			return post(request, (List<?>) null, null);
		}
		body = body.trim();
		Object input;
		if (body.startsWith("[")) {
			input = mapper.toList(body, inspector.getInputType(function));
		}
		else {
			if (body.startsWith("{")) {
				input = mapper.toSingle(body, inspector.getInputType(function));
			}
			else if (body.startsWith("\"")) {
				input = body.substring(1, body.length() - 2);
			}
			else {
				input = converter.convert(function, body);
			}
		}
		return post(request, input);
	}

	private Mono<ResponseEntity<?>> post(ServerWebExchange request,
			Object body) {
		if (body instanceof List) {
			return post(request, (List<?>) body, null);
		}
		request.getAttributes().put(WebRequestConstants.INPUT_SINGLE, true);
		return post(request, Collections.singletonList(body), null);
	}

	private Mono<ResponseEntity<?>> post(ServerWebExchange request,
			List<?> body, MultiValueMap<String, String> params) {

		@SuppressWarnings("unchecked")
		Function<Publisher<?>, Publisher<?>> function = (Function<Publisher<?>, Publisher<?>>) request
				.getAttribute(WebRequestConstants.FUNCTION);
		@SuppressWarnings("unchecked")
		Consumer<Publisher<?>> consumer = (Consumer<Publisher<?>>) request
				.getAttribute(WebRequestConstants.CONSUMER);
		Boolean single = (Boolean) request.getAttribute(WebRequestConstants.INPUT_SINGLE);

		FluxFormRequest form = FluxFormRequest
				.from(request.getRequest().getQueryParams());
		if (params != null) {
			form.body().putAll(params);
		}

		Flux<?> flux = body == null ? form.flux() : Flux.fromIterable(body);
		if (debug) {
			flux = flux.log();
		}
		if (inspector.isMessage(function)) {
			flux = messages(request, function == null ? consumer : function, flux);
		}
		if (function != null) {
			Flux<?> result = Flux.from(function.apply(flux));
			return response(request, function, result, single, false);
		}

		if (consumer != null) {
			consumer.accept(flux);
			if (logger.isDebugEnabled()) {
				logger.debug("Handled POST with consumer");
			}
			return Mono.just(ResponseEntity.status(HttpStatus.ACCEPTED).build());
		}

		throw new IllegalArgumentException("no such function");
	}

	private Flux<?> messages(ServerWebExchange request, Object function, Flux<?> flux) {
		Map<String, Object> headers = HeaderUtils
				.fromHttp(request.getRequest().getHeaders());
		flux = flux.map(payload -> MessageUtils.create(function, payload, headers));
		return flux;
	}

	private void addHeaders(BodyBuilder builder, Message<?> message) {
		HttpHeaders headers = new HttpHeaders();
		builder.headers(HeaderUtils.fromMessage(message.getHeaders(), headers));
	}

	private Mono<ResponseEntity<?>> response(ServerWebExchange request,
			Object handler, Publisher<?> result, Boolean single, boolean getter) {

		BodyBuilder builder = ResponseEntity.ok();
		if (inspector.isMessage(handler)) {
			result = Flux.from(result)
					.doOnNext(value -> addHeaders(builder, (Message<?>) value))
					.map(message -> MessageUtils.unpack(handler, message).getPayload());
		}

		if (single != null && single && isOutputSingle(handler)) {
			result = Mono.from(result);
		}
		else if (getter && single == null && isOutputSingle(handler)) {
			result = Mono.from(result);
		}
		else if (isInputMultiple(handler) && isOutputSingle(handler)) {
			result = Mono.from(result);
		}
		Publisher<?> output = result;
		if (result instanceof Mono) {
			return Mono.from(output).flatMap(body -> Mono.just(builder.body(body)));
		}
		return Flux.from(result).collectList()
				.flatMap(body -> Mono.just(builder.body(body)));
	}

	private boolean isInputMultiple(Object handler) {
		Class<?> type = inspector.getInputType(handler);
		Class<?> wrapper = inspector.getInputWrapper(handler);
		return Collection.class.isAssignableFrom(type) || Flux.class.equals(wrapper);
	}

	private boolean isOutputSingle(Object handler) {
		Class<?> type = inspector.getOutputType(handler);
		Class<?> wrapper = inspector.getOutputWrapper(handler);
		if (Stream.class.isAssignableFrom(type)) {
			return false;
		}
		if (wrapper == type) {
			return true;
		}
		return Mono.class.equals(wrapper) || Optional.class.equals(wrapper);
	}

	@GetMapping(path = "/**")
	@ResponseBody
	public Mono<ResponseEntity<?>> get(ServerWebExchange request) {
		@SuppressWarnings("unchecked")
		Function<Publisher<?>, Publisher<?>> function = (Function<Publisher<?>, Publisher<?>>) request
				.getAttribute(WebRequestConstants.FUNCTION);
		@SuppressWarnings("unchecked")
		Supplier<Publisher<?>> supplier = (Supplier<Publisher<?>>) request
				.getAttribute(WebRequestConstants.SUPPLIER);
		String argument = (String) request.getAttribute(WebRequestConstants.ARGUMENT);

		if (function != null) {
			return response(request, function, value(function, argument), true, true);
		}
		else {
			return response(request, supplier, supplier(supplier), null, true);
		}
	}

	private Publisher<?> supplier(Supplier<Publisher<?>> supplier) {
		Publisher<?> result = supplier.get();
		if (logger.isDebugEnabled()) {
			logger.debug("Handled GET with supplier");
		}
		return debug ? Flux.from(result).log() : result;
	}

	private Mono<?> value(Function<Publisher<?>, Publisher<?>> function, String value) {
		Object input = converter.convert(function, value);
		Mono<?> result = Mono.from(function.apply(Flux.just(input)));
		if (logger.isDebugEnabled()) {
			logger.debug("Handled GET with function");
		}
		return debug ? result.log() : result;
	}
}
