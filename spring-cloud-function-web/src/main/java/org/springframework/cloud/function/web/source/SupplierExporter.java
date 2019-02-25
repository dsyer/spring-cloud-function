/*
 * Copyright 2012-2019 the original author or authors.
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

package org.springframework.cloud.function.web.source;

import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.context.SmartLifecycle;
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.Message;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Forwards items obtained from a {@link Supplier} or set of suppliers to an external HTTP
 * endpoint.
 *
 * @author Dave Syer
 *
 */
public class SupplierExporter implements SmartLifecycle {

	private static Log logger = LogFactory.getLog(SupplierExporter.class);

	private final FunctionCatalog catalog;

	private final WebClient client;

	private final DestinationResolver destinationResolver;

	private final RequestBuilder requestBuilder;

	private final String supplier;

	private volatile boolean running;

	private volatile boolean ok = true;

	private boolean autoStartup = true;

	private boolean debug = true;

	private volatile Disposable subscription;

	SupplierExporter(RequestBuilder requestBuilder,
			DestinationResolver destinationResolver, FunctionCatalog catalog,
			WebClient client, ExporterProperties props) {
		this.requestBuilder = requestBuilder;
		this.destinationResolver = destinationResolver;
		this.catalog = catalog;
		this.client = client;
		this.debug = props.isDebug();
		this.autoStartup = props.isAutoStartup();
		this.supplier = props.getSink().getName();
	}

	@Override
	public void start() {
		if (this.running) {
			return;
		}

		this.running = true;
		this.ok = true;
		logger.info("Starting");

		Flux<Object> streams = Flux.empty();
		Set<String> names = this.supplier == null ? this.catalog.getNames(Supplier.class)
				: Collections.singleton(this.supplier);
		for (String name : names) {
			Supplier<Flux<Object>> supplier = this.catalog.lookup(Supplier.class, name);
			if (supplier == null) {
				logger.warn("No such Supplier: " + name);
				continue;
			}
			streams = streams.mergeWith(forward(supplier, name));
		}

		this.subscription = streams.doOnError(error -> {
			this.ok = false;
			if (!this.debug) {
				logger.info(error);
			}
		}).doOnTerminate(() -> this.running = false).doOnNext(value -> {
			if (this.subscription != null && !this.running) {
				this.subscription.dispose();
			}
		}).subscribe();
	}

	private Flux<ClientResponse> forward(Supplier<Flux<Object>> supplier, String name) {
		return supplier.get().flatMap(value -> {
			String destination = this.destinationResolver.destination(supplier, name,
					value);
			if (this.debug) {
				logger.info("Posting to: " + destination);
			}
			return post(uri(destination), destination, value);
		});
	}

	private Mono<ClientResponse> post(URI uri, String destination, Object value) {
		Object body = value;
		if (value instanceof Message) {
			Message<?> message = (Message<?>) value;
			body = message.getPayload();
		}
		Mono<ClientResponse> result = this.client.post().uri(uri)
				.headers(headers -> headers(headers, destination, value)).syncBody(body)
				.exchange();
		if (this.debug) {
			result = result.log();
		}
		return result;
	}

	private void headers(HttpHeaders headers, String destination, Object value) {
		headers.putAll(this.requestBuilder.headers(destination, value));
	}

	private URI uri(String destination) {
		return this.requestBuilder.uri(destination);
	}

	public boolean isOk() {
		return this.ok;
	}

	@Override
	public void stop() {
		logger.info("Stopping");
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

}
