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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.InstantiationUtil;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A basic builder implementation to build {@link TableDescriptor}.
 */
@PublicEvolving
public abstract class TableDescriptorBuilder<DESCRIPTOR extends TableDescriptor, BUILDER extends TableDescriptorBuilder<DESCRIPTOR, BUILDER>> {

	private final DESCRIPTOR descriptor;

	protected TableDescriptorBuilder(Class<DESCRIPTOR> descriptorClass) {
		descriptor = InstantiationUtil.instantiate(descriptorClass, TableDescriptor.class);
	}

	/**
	 * Returns the this builder instance in the type of subclass.
	 */
	protected abstract BUILDER self();

	/**
	 * Specifies the table schema.
	 */
	public BUILDER schema(Schema schema) {
		descriptor.schema = schema;
		return self();
	}

	/**
	 * Specifies the partition keys of this table.
	 */
	public BUILDER partitionedBy(String... fieldNames) {
		checkArgument(descriptor.partitionedFields.isEmpty(), "partitionedBy(...) shouldn't be called more than once.");
		descriptor.partitionedFields.addAll(Arrays.asList(fieldNames));
		return self();
	}

	/**
	 * Extends some parts from the original registered table path.
	 */
	public BUILDER like(String tablePath, LikeOption... likeOptions) {
		descriptor.likePath = tablePath;
		descriptor.likeOptions = likeOptions;
		return self();
	}

	protected BUILDER option(String key, String value) {
		descriptor.options.put(key, value);
		return self();
	}

	/**
	 * Returns created table descriptor.
	 */
	public DESCRIPTOR build() {
		return descriptor;
	}
}
