/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptor;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

public class Connector extends ConnectorDescriptor {

	private final Map<String, String> options = new HashMap<>();

	/**
	 * Constructs a {@link org.apache.flink.table.descriptors.ConnectorDescriptor}.
	 *
	 * @param identifier string that identifies this connector
	 */
	public Connector(String identifier) {
		this.options.put(CONNECTOR.key(), identifier);
	}

	public Connector option(String key, String value) {
		String lowerKey = key.toLowerCase().trim();
		if (CONNECTOR.key().equals(lowerKey)) {
			throw new IllegalArgumentException("It's not allowed to override 'connector' option.");
		}
		options.put(key, value);
		return this;
	}

	/**
	 * Converts this descriptor into a set of connector options.
	 */
	protected Map<String, String> toConnectorOptions() {
		return new HashMap<>(options);
	}

}
