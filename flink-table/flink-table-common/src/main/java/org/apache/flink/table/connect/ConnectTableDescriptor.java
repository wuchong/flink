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

import org.apache.flink.table.descriptors.Descriptor;

import java.util.Map;

public interface ConnectTableDescriptor extends Descriptor {

	ConnectTableDescriptor schema(Schema schema);

	ConnectTableDescriptor partitionedBy(String... fieldNames);

	ConnectTableDescriptor option(String key, String value);

	ConnectTableDescriptor options(Map<String, String> options);

	/**
	 * Registers the table described by underlying properties in a given path.
	 *
	 * <p>There is no distinction between source and sink at the descriptor level anymore as this
	 * method does not perform actual class lookup. It only stores the underlying properties. The
	 * actual source/sink lookup is performed when the table is used.
	 *
	 * <p>Temporary objects can shadow permanent ones. If a permanent object in a given path exists, it will
	 * be inaccessible in the current session. To make the permanent object available again you can drop the
	 * corresponding temporary object.
	 *
	 * <p><b>NOTE:</b> The schema must be explicitly defined.
	 *
	 * @param path path where to register the temporary table
	 */
	void createTemporaryTable(String path);
}
