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

import javax.servlet.http.HttpServletRequest;

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
import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.BodyBuilder;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.WebRequest;

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

	@PostMapping(path = "/**")
	@ResponseBody
	public Mono<ResponseEntity<Publisher<?>>> post(NativeWebRequest request,
			@RequestBody(required = false) String body) {
		Object function = request.getAttribute(WebRequestConstants.FUNCTION,
				WebRequest.SCOPE_REQUEST);
		if (function == null) {
			function = request.getAttribute(WebRequestConstants.CONSUMER,
					WebRequest.SCOPE_REQUEST);
		}
		if (!StringUtils.hasText(body)) {
			return post(request, (List<?>) null);
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

	private Mono<ResponseEntity<Publisher<?>>> post(NativeWebRequest request,
			Object body) {
		if (body instanceof List) {
			return post(request, (List<?>) body);
		}
		request.setAttribute(WebRequestConstants.INPUT_SINGLE, true,
				WebRequest.SCOPE_REQUEST);
		return post(request, Collections.singletonList(body));
	}

	private Mono<ResponseEntity<Publisher<?>>> post(NativeWebRequest request,
			List<?> body) {

		@SuppressWarnings("unchecked")
		Function<Publisher<?>, Publisher<?>> function = (Function<Publisher<?>, Publisher<?>>) request
				.getAttribute(WebRequestConstants.FUNCTION, WebRequest.SCOPE_REQUEST);
		@SuppressWarnings("unchecked")
		Consumer<Publisher<?>> consumer = (Consumer<Publisher<?>>) request
				.getAttribute(WebRequestConstants.CONSUMER, WebRequest.SCOPE_REQUEST);
		Boolean single = (Boolean) request.getAttribute(WebRequestConstants.INPUT_SINGLE,
				WebRequest.SCOPE_REQUEST);

		FluxFormRequest<?, ?> form = FluxFormRequest.from(request.getParameterMap());

		Flux<?> flux = body == null ? form.flux() : Flux.fromIterable(body);
		if (debug) {
			flux = flux.log();
		}
		if (inspector.isMessage(function)) {
			flux = messages(request, function==null ? consumer : function, flux);
		}
		if (function != null) {
			Flux<?> result = Flux.from(function.apply(flux));
			BodyBuilder builder = ResponseEntity.ok();
			if (inspector.isMessage(function)) {
				result = result
						.doOnNext(value -> addHeaders(builder, (Message<?>) value));
				result = result.map(
						message -> MessageUtils.unpack(function, message).getPayload());
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Handled POST with function");
			}
			Publisher<?> response = response(request, function, single, result);
			return Flux.from(response)
					.then(Mono.fromSupplier(() -> builder.body(response)));
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

	private Flux<?> messages(NativeWebRequest request, Object function, Flux<?> flux) {
		Map<String, Object> headers = HeaderUtils.fromHttp(new ServletServerHttpRequest(
				request.getNativeRequest(HttpServletRequest.class)).getHeaders());
		flux = flux.map(payload -> MessageUtils.create(function, payload, headers));
		return flux;
	}

	private void addHeaders(BodyBuilder builder, Message<?> message) {
		HttpHeaders headers = new HttpHeaders();
		builder.headers(HeaderUtils.fromMessage(message.getHeaders(), headers));
	}

	private Publisher<?> response(WebRequest request, Object handler, Boolean single,
			Publisher<?> result) {

		if (single != null && single && isOutputSingle(handler)) {
			request.setAttribute(WebRequestConstants.OUTPUT_SINGLE, true,
					WebRequest.SCOPE_REQUEST);
			return Mono.from(result);
		}

		if (isInputMultiple(handler) && isOutputSingle(handler)) {
			request.setAttribute(WebRequestConstants.OUTPUT_SINGLE, true,
					WebRequest.SCOPE_REQUEST);
			return Mono.from(result);
		}

		request.setAttribute(WebRequestConstants.OUTPUT_SINGLE, false,
				WebRequest.SCOPE_REQUEST);

		return result;
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
	public ResponseEntity<Publisher<?>> get(WebRequest request) {
		@SuppressWarnings("unchecked")
		Function<Publisher<?>, Publisher<?>> function = (Function<Publisher<?>, Publisher<?>>) request
				.getAttribute(WebRequestConstants.FUNCTION, WebRequest.SCOPE_REQUEST);
		@SuppressWarnings("unchecked")
		Supplier<Publisher<?>> supplier = (Supplier<Publisher<?>>) request
				.getAttribute(WebRequestConstants.SUPPLIER, WebRequest.SCOPE_REQUEST);
		String argument = (String) request.getAttribute(WebRequestConstants.ARGUMENT,
				WebRequest.SCOPE_REQUEST);

		Publisher<?> result;
		if (function != null) {
			result = value(function, argument);
		}
		else {
			result = response(request, supplier, true, supplier(supplier));
		}
		if (inspector.isMessage(function)) {
			result = Flux.from(result)
					.map(message -> MessageUtils.unpack(function, message));
		}
		return ResponseEntity.ok().body(result);
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
