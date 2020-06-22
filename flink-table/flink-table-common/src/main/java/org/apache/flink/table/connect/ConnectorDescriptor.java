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

package org.apache.flink.table.connect;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorBase;

import java.util.Map;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/**
 * Describes a connector to an other system.
 */
@PublicEvolving
public abstract class ConnectorDescriptor extends DescriptorBase implements Descriptor {

	private String identifier;

	/**
	 * Constructs a {@link org.apache.flink.table.descriptors.ConnectorDescriptor}.
	 *
	 * @param identifier string that identifies this connector
	 */
	public ConnectorDescriptor(String identifier) {
		this.identifier = identifier;
	}

	@Override
	public final Map<String, String> toProperties() {
		Configuration configuration = new Configuration();
		configuration.set(CONNECTOR, identifier);
		configuration.set(PROPERTY_VERSION, 1);
		toConnectorOptions().forEach(configuration::setString);
		return configuration.toMap();
	}

	/**
	 * Converts this descriptor into a set of connector options.
	 */
	protected abstract Map<String, String> toConnectorOptions();
}
