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

package org.apache.flink.table.descriptor2;

import org.apache.flink.annotation.PublicEvolving;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Describes a table to connect. It is a same representation of SQL CREATE TABLE DDL.
 */
@PublicEvolving
public abstract class TableDescriptor {

	// Partition keys if this is a partitioned table. It's an empty set if the table is not partitioned
	final List<String> partitionedFields = new ArrayList<>();
	Schema schema;

	public TableDescriptor schema(Schema schema) {
		this.schema = schema;
		return this;
	}

	public TableDescriptor partitionedBy(String... fieldNames) {
		checkArgument(partitionedFields.isEmpty(), "partitionedBy(...) shouldn't be called more than once.");
		partitionedFields.addAll(Arrays.asList(fieldNames));
		return this;
	}

	public TableDescriptor like(String tablePath, LikeOption... likeOptions) {
		return this;
	}

	/**
	 * Converts this descriptor into a set of connector options.
	 */
	protected abstract Map<String, String> toConnectorOptions();
}
