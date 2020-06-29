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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

public class TableDescriptor {

	// Partition keys if this is a partitioned table. It's an empty set if the table is not partitioned
	private final List<String> partitionedFields = new ArrayList<>();
	private Map<String, String> options;
	private Schema schema;

	public TableDescriptor connector(ConnectorDescriptor connectorDescriptor) {
		this.options = connectorDescriptor.toConnectorOptions();
		return this;
	}

	public TableDescriptor schema(Schema schema) {
		this.schema = schema;
		return this;
	}

	public TableDescriptor partitionedBy(String... fieldNames) {
		checkArgument(partitionedFields.isEmpty(), "partitionedBy(...) shouldn't be called more than once.");
		partitionedFields.addAll(Arrays.asList(fieldNames));
		return this;
	}

	// TODO: only visible for V3
	public void createTemporaryTable(String tablePath) {
		// nothing
	}

}
