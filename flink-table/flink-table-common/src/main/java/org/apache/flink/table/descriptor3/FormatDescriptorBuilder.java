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

/**
 * Describes a connector to an other system.
 */
@PublicEvolving
public abstract class FormatDescriptorBuilder {

	private final FormatDescriptor descriptor;

	protected FormatDescriptorBuilder(Class<? extends FormatDescriptor> descriptorClass) {
		descriptor = InstantiationUtil.instantiate(descriptorClass, FormatDescriptor.class);
	}

	protected FormatDescriptorBuilder option(String key, String value) {
		descriptor.formatOptions.put(key, value);
		return this;
	}

	public FormatDescriptor build() {
		return descriptor;
	}
}
