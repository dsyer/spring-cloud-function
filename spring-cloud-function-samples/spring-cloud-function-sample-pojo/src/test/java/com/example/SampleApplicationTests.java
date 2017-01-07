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
package com.example;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.example.SampleApplicationTests.WebSocketConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.reactive.socket.server.upgrade.JettyRequestUpgradeStrategy;

import static org.assertj.core.api.Assertions.assertThat;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

/**
 * @author Dave Syer
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { SampleApplication.class,
		WebSocketConfiguration.class }, webEnvironment = WebEnvironment.RANDOM_PORT)
public class SampleApplicationTests {

	@LocalServerPort
	private int port;

	@Test
	public void websocket() throws Exception {
		WebSocketClient client = new ReactorNettyWebSocketClient();
		ReplayProcessor<String> output = ReplayProcessor.create();
		client.execute(new URI("ws://localhost:" + port + "/ws"),
				session -> session.send(Mono.just(session.textMessage("Hello")))
						.then(session.receive().map(WebSocketMessage::getPayloadAsText)
								.subscribeWith(output).take(1).then()))
				.blockMillis(5000);
		assertThat(output.collectList().block()).contains("[Hello]");
	}

	@Test
	public void words() {
		assertThat(new TestRestTemplate()
				.getForObject("http://localhost:" + port + "/words", String.class))
						.isEqualTo("{\"value\":\"foo\"}{\"value\":\"bar\"}");
	}

	@Test
	public void uppercase() {
		assertThat(new TestRestTemplate().postForObject(
				"http://localhost:" + port + "/uppercase", "{\"value\":\"foo\"}",
				String.class)).isEqualTo("{\"value\":\"FOO\"}");
	}

	@ConditionalOnClass(WebSocketHandlerAdapter.class)
	@Configuration
	static class WebSocketConfiguration {

		@Autowired
		private WebSocketHandler uppercaseHandler;

		@Bean
		public HandlerMapping webSocketMapping() {
			Map<String, WebSocketHandler> map = new HashMap<>();
			map.put("/ws", uppercaseHandler);

			SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
			mapping.setUrlMap(map);
			mapping.setOrder(-1);
			return mapping;
		}

		@Bean
		protected WebSocketHandler uppercaseHandler(ObjectMapper mapper, 
				@Qualifier("uppercase") Function<Flux<Foo>, Flux<Bar>> uppercase) {
			return session -> {
				Flux<WebSocketMessage> received = session.receive();
				return session.send(
						received.map(message -> "[" + message.getPayloadAsText() + "]")
								.log().map(text -> session.textMessage(text)));
			};
		}

		@Bean
		public WebSocketHandlerAdapter handlerAdapter() {
			return new WebSocketHandlerAdapter(
					new HandshakeWebSocketService(new JettyRequestUpgradeStrategy()));
		}
	}

}
