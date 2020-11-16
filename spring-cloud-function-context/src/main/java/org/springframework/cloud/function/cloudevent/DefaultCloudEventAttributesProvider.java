/*
 * Copyright 2020-2020 the original author or authors.
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

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.StringUtils;

/**
 * @author Oleg Zhurakousky
 * @since 3.1
 *
 */
public class DefaultCloudEventAttributesProvider implements CloudEventAttributesProvider, ApplicationContextAware {

	private ConfigurableApplicationContext applicationContext;

	@Override
	public CloudEventAttributes update(CloudEventAttributes attributes) {
		return attributes.setSource(this.getApplicationName());
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	private String getApplicationName() {
		ConfigurableEnvironment environment = this.applicationContext.getEnvironment();
		String name = environment.getProperty("spring.application.name");
		return "http://spring.io/"
				+ (StringUtils.hasText(name) ? name : "application-" + this.applicationContext.getId());
	}

}
