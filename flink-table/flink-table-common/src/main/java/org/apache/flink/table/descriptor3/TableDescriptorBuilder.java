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

package org.apache.flink.table.descriptor3;

import org.apache.flink.util.InstantiationUtil;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

public abstract class TableDescriptorBuilder {

	private final TableDescriptor descriptor;

	protected TableDescriptorBuilder(Class<? extends TableDescriptor> descriptorClass) {
		descriptor = InstantiationUtil.instantiate(descriptorClass, TableDescriptor.class);
	}

	public TableDescriptorBuilder schema(Schema schema) {
		descriptor.schema = schema;
		return this;
	}

	public TableDescriptorBuilder partitionedBy(String... fieldNames) {
		checkArgument(descriptor.partitionedFields.isEmpty(), "partitionedBy(...) shouldn't be called more than once.");
		descriptor.partitionedFields.addAll(Arrays.asList(fieldNames));
		return this;
	}

	public TableDescriptorBuilder like(String tablePath, LikeOption... likeOptions) {
		descriptor.likePath = tablePath;
		descriptor.likeOptions = likeOptions;
		return this;
	}

	protected TableDescriptorBuilder option(String key, String value) {
		descriptor.options.put(key, value);
		return this;
	}

	public TableDescriptor build() {
		return descriptor;
	}

}
