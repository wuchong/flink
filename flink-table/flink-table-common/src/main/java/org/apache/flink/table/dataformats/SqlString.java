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
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.CharType;
import java.io.Serializable;

/**
 * {@link SqlString} is a data structure represents data of type {@link VarCharType} and {@link CharType}
 * in table internal implementation.
 */
@PublicEvolving
public interface SqlString extends Comparable<SqlString>, Serializable {

	/**
	 * Get the underlying UTF-8 byte array, the returned bytes may be reused.
	 */
	byte[] getBytes();

	/**
	 * Converts this {@link SqlString} object to a Java {@link String}.
	 */
	String getJavaString();

	// ------------------------------------------------------------------------------------------
    // Constructor helper
    // ------------------------------------------------------------------------------------------

	/**
	 * Creates an BinaryString from given java String.
	 */
	static SqlString fromString(String str) {
		return LazyBinaryString.fromString(str);
	}

	/**
	 * Creates a {@link SqlString} from the given UTF-8 bytes.
	 */
	static SqlString fromBytes(byte[] bytes) {
		return LazyBinaryString.fromBytes(bytes);
	}

	/**
	 * Creates a {@link SqlString} from the given UTF-8 bytes with offset and number of bytes.
	 */
	static SqlString fromBytes(byte[] bytes, int offset, int numBytes) {
		return LazyBinaryString.fromBytes(bytes, offset, numBytes);
	}
}
