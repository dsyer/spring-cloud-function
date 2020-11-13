/*
 * Copyright 2019-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.function.cloudevent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Oleg Zhurakousky
 * @since 3.1
 *
 */
public class DefaultCloudEventAttributesProvider implements CloudEventAtttributesProvider, ApplicationContextAware {

	private String defaultSource = "https://spring.io/default-source";

	private String defaultType = "spring.io.DefaultEventType";

	private String source;

	private String type;

	/**
	 * @param source the source to set
	 */
	public void setSource(String source) {
		this.source = source;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	public void setDefaultSource(String defaultSource) {
		this.defaultSource = defaultSource;
	}

	public void setDefaultType(String defaultType) {
		this.defaultType = defaultType;
	}

	@Override
	public CloudEventAttributes get(String ce_id, String ce_specversion, String ce_source, String ce_type) {
		Assert.hasText(ce_id, "'ce_id' must not be null or empty");
		Assert.hasText(ce_specversion, "'ce_specversion' must not be null or empty");
		Assert.hasText(ce_source, "'ce_source' must not be null or empty");
		Assert.hasText(ce_type, "'ce_type' must not be null or empty");
		Map<String, Object> requiredAttributes = new HashMap<>();
		requiredAttributes.put(CloudEventMessageUtils.CE_ID, ce_id);
		requiredAttributes.put(CloudEventMessageUtils.CE_SPECVERSION, ce_specversion);
		requiredAttributes.put(CloudEventMessageUtils.CE_SOURCE, ce_source);
		requiredAttributes.put(CloudEventMessageUtils.CE_TYPE, ce_type);
		return new CloudEventAttributes(requiredAttributes);
	}

	@Override
	public CloudEventAttributes get(String ce_source, String ce_type) {
		return this.get(UUID.randomUUID().toString(), "1.0", ce_source, ce_type);
	}

	/**
	 * By default it will copy all the headers while exposing accessor to allow user to
	 * modify any of them.
	 */
	@Override
	public RequiredAttributeAccessor get(MessageHeaders headers) {
		return new RequiredAttributeAccessor(headers).setSource(getSource(headers)).setType(getType(null));
	}

	@Override
	public Map<String, Object> generateDefaultCloudEventHeaders(Message<?> inputMessage, Object result) {
		if (inputMessage.getHeaders().containsKey(CloudEventMessageUtils.CE_ID)) {
			// input is a cloud event
			return this.get(inputMessage.getHeaders()).setId(inputMessage.getHeaders().getId().toString())
					.setType(getType(result)).setSource(getSource(inputMessage.getHeaders()));
		}
		return Collections.emptyMap();
	}

	private String getSource(MessageHeaders headers) {
		if (this.source != null) {
			return this.source;
		}
		if (headers.containsKey(CloudEventMessageUtils.CE_SOURCE)) {
			return headers.get(CloudEventMessageUtils.CE_SOURCE, String.class);
		}
		return this.defaultSource;
	}

	private String getType(Object result) {
		if (this.type != null) {
			return this.type;
		}
		// TODO: maybe use default type if result is a generic Map type
		return result == null ? this.defaultType : result.getClass().getName();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		if (this.source == null) {
			this.source = getApplicationName((ConfigurableApplicationContext) applicationContext);
		}
	}

	private String getApplicationName(ConfigurableApplicationContext applicationContext) {
		ConfigurableEnvironment environment = applicationContext.getEnvironment();
		String name = environment.getProperty("spring.application.name");
		return "http://spring.io/" + (StringUtils.hasText(name) ? name : applicationContext.getId());
	}

}
