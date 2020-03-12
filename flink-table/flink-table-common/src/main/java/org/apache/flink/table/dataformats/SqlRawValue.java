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

package org.apache.flink.table.dataformats;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.logical.RawType;

import java.io.Serializable;

/**
 * {@link SqlRawValue} is a data structure represents data of type {@link RawType}
 * in table internal implementation. This class is used to encapsulate "raw value",
 * the "raw value" might be in binary format, i.e. byte[], or in Java object.
 */
@PublicEvolving
public interface SqlRawValue<T> extends Serializable {

	/**
	 * Converts a {@link SqlRawValue} into a Java object, the {@code serializer} is required because
	 * the "raw value" might be in binary format which can be deserialized by the {@code serializer}.
	 *
	 * Note: the returned Java object may be reused.
	 */
	T getJavaObject(TypeSerializer<T> serializer);

	/**
	 * Converts a {@link SqlRawValue} into a byte array, the {@code serializer} is required because
	 * the "raw value" might be in Java object format which can be serialized by the {@code serializer}.
	 *
	 * Note: the returned bytes may be reused.
	 */
	byte[] getBytes(TypeSerializer<T> serializer);

	// ------------------------------------------------------------------------------------------
	// Constructor helper
	// ------------------------------------------------------------------------------------------

	/**
	 * Creates a {@link SqlRawValue} instance from a java object.
	 */
	static <T> SqlRawValue<T> fromJavaObject(T javaObject) {
		return new LazyBinarySqlRawValue<>(javaObject);
	}

}
