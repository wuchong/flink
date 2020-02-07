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

package org.apache.flink.table.factories;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * .
 */
public interface ConnectorFactory extends TableFactory {

	String CONNECTOR_TYPE = "connector.type";
	String CONNECTOR_VERSION = "connector.version";

	String connectorType();

	Optional<String> connectorVersion();

	@Override
	default Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, connectorType());
		connectorVersion().ifPresent(v -> context.put(CONNECTOR_VERSION, v));
		return context;
	}
}
