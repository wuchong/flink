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

@PublicEvolving
public enum ChangelogKind {
	/**
	 * Insertion operation.
	 */
	INSERT(0),

	/**
	 * Previous content of an updated row.
	 */
	UPDATE_BEFORE(1),

	/**
	 * New content of an updated row.
	 */
	UPDATE_AFTER(2),

	/**
	 * Deletion operation.
	 */
	DELETE(3);

	private final byte value;

	ChangelogKind(int value) {
		this.value = (byte) value;
	}

	public byte getValue() {
		return value;
	}

	public static ChangelogKind valueOf(byte value) {
		switch (value) {
			case 0: return INSERT;
			case 1: return UPDATE_BEFORE;
			case 2: return UPDATE_AFTER;
			case 3: return DELETE;
			default: throw new IllegalArgumentException("Unsupported ChangelogKind value " + value);
		}
	}
}
