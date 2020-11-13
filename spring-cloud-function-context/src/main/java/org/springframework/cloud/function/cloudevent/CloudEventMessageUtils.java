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

import java.util.HashMap;
import java.util.Map;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * Miscellaneous utility methods to deal with Cloud Events - https://cloudevents.io/. <br>
 * Mainly for internal use within the framework;
 *
 * @author Oleg Zhurakousky
 * @since 3.1
 */
public final class CloudEventMessageUtils {

	private CloudEventMessageUtils() {

	}

	/**
	 * String value of 'application/cloudevents' mime type.
	 */
	public static String APPLICATION_CLOUDEVENTS_VALUE = "application/cloudevents";

	/**
	 * {@link MimeType} instance representing 'application/cloudevents' mime type.
	 */
	public static MimeType APPLICATION_CLOUDEVENTS = MimeTypeUtils.parseMimeType(APPLICATION_CLOUDEVENTS_VALUE);

	/**
	 * Prefix for attributes.
	 */
	public static String ATTR_PREFIX = "ce_";

	/**
	 * Prefix for attributes.
	 */
	public static String HTTP_PREFIX = "ce-";

	/**
	 * Value for 'data' attribute.
	 */
	public static String DATA = "data";

	/**
	 * Value for 'data' attribute with prefix.
	 */
	public static String CE_DATA = ATTR_PREFIX + DATA;

	/**
	 * Value for 'id' attribute.
	 */
	public final static String ID = "id";

	/**
	 * Value for 'id' attribute with prefix.
	 */
	public static String CE_ID = ATTR_PREFIX + ID;

	public final static String HTTP_ID = HTTP_PREFIX + ID;

	/**
	 * Value for 'source' attribute.
	 */
	public final static String SOURCE = "source";

	public final static String HTTP_SOURCE = HTTP_PREFIX + SOURCE;

	/**
	 * Value for 'source' attribute with prefix.
	 */
	public static String CE_SOURCE = ATTR_PREFIX + SOURCE;

	/**
	 * Value for 'specversion' attribute.
	 */
	public final static String SPECVERSION = "specversion";

	public final static String HTTP_SPECVERSION = HTTP_PREFIX + SPECVERSION;

	/**
	 * Value for 'specversion' attribute with prefix.
	 */
	public static String CE_SPECVERSION = ATTR_PREFIX + SPECVERSION;

	/**
	 * Value for 'type' attribute.
	 */
	public final static String TYPE = "type";

	public final static String HTTP_TYPE = HTTP_PREFIX + TYPE;

	/**
	 * Value for 'type' attribute with prefix.
	 */
	public static String CE_TYPE = ATTR_PREFIX + TYPE;

	/**
	 * Value for 'datacontenttype' attribute.
	 */
	public static String DATACONTENTTYPE = "datacontenttype";

	/**
	 * Value for 'datacontenttype' attribute with prefix.
	 */
	public static String CE_DATACONTENTTYPE = ATTR_PREFIX + DATACONTENTTYPE;

	/**
	 * Value for 'dataschema' attribute.
	 */
	public static String DATASCHEMA = "dataschema";

	/**
	 * Value for 'dataschema' attribute with prefix.
	 */
	public static String CE_DATASCHEMA = ATTR_PREFIX + DATASCHEMA;

	/**
	 * Value for 'subject' attribute.
	 */
	public static String SUBJECT = "subject";

	/**
	 * Value for 'subject' attribute with prefix.
	 */
	public static String CE_SUBJECT = ATTR_PREFIX + SUBJECT;

	/**
	 * Value for 'time' attribute.
	 */
	public static String TIME = "time";

	/**
	 * Value for 'time' attribute with prefix.
	 */
	public static String CE_TIME = ATTR_PREFIX + TIME;

	/**
	 * Checks if {@link Message} represents cloud event in binary-mode.
	 */
	public static boolean isBinary(Map<String, Object> headers) {
		return (headers.containsKey(CE_ID) && headers.containsKey(CE_SOURCE) && headers.containsKey(CE_SPECVERSION)
				&& headers.containsKey(CE_TYPE));
	}

	public static MessageHeaders canonicalize(MessageHeaders headers) {
		Map<String, Object> result = new HashMap<String, Object>();
		for (String key : headers.keySet()) {
			switch (key) {
			case SOURCE:
				result.putIfAbsent(CE_SOURCE, headers.get(key));
				break;

			case TYPE:
				result.putIfAbsent(CE_TYPE, headers.get(key));
				break;

			case SPECVERSION:
				result.putIfAbsent(CE_SPECVERSION, headers.get(key));
				break;

			case ID:
				result.putIfAbsent(CE_ID, headers.get(key));
				break;

			default:
				if (key.startsWith(HTTP_PREFIX)) {
					result.put(key.replace(HTTP_PREFIX, ATTR_PREFIX), headers.get(key));
				}
				else {
					result.put(key, headers.get(key));
				}
				break;
			}
		}
		return new MessageHeaders(result);
	}

	public static MessageHeaders http(MessageHeaders headers) {
		Map<String, Object> result = new HashMap<String, Object>();
		for (String key : headers.keySet()) {
			if (key.startsWith(ATTR_PREFIX)) {
				result.put(key.replace(ATTR_PREFIX, HTTP_PREFIX), headers.get(key));
			}
			else {
				result.put(key, headers.get(key));
			}
		}
		return new MessageHeaders(result);
	}

}
