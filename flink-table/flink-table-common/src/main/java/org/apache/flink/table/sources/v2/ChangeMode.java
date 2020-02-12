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

package org.apache.flink.table.sources.v2;

import org.apache.flink.table.dataformat.ChangeType;

/**
 * .
 */
public enum ChangeMode {

	/**
	 * Only contains {@link ChangeType#INSERT} messages.
	 */
	INSERT_ONLY(true, false, false, false),

	/**
	 * Contains {@link ChangeType#INSERT}, {@link ChangeType#DELETE} and {@link ChangeType#UPDATE_NEW} messages,
	 * but without {@link ChangeType#UPDATE_OLD}.
	 */
	IGNORE_UPDATE_OLD(true, true, false, true),

	/**
	 * Contains all possible {@link ChangeType} messages.
	 */
	FULL_CHANGE(true, true, true, true);

	// TODO: will introduce more modes in the future, e.g. without delete messages.

	private final boolean hasInsert;
	private final boolean hasDelete;
	private final boolean hasUpdateOld;
	private final boolean hasUpdateNew;

	private ChangeMode(boolean hasInsert, boolean hasDelete, boolean hasUpdateOld, boolean hasUpdateNew) {
		this.hasInsert = hasInsert;
		this.hasDelete = hasDelete;
		this.hasUpdateOld = hasUpdateOld;
		this.hasUpdateNew = hasUpdateNew;
	}

	public boolean containsInsertMessages() {
		return hasInsert;
	}

	public boolean containsDeleteMessage() {
		return hasDelete;
	}

	public boolean containsUpdateOldMessages() {
		return hasUpdateOld;
	}

	public boolean containsUpdateNewMessage() {
		return hasUpdateNew;
	}
}
