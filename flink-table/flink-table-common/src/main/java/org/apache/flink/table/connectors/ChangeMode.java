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

package org.apache.flink.table.connectors;

import org.apache.flink.table.connectors.ChangelogRow.Kind;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * The set of changes a {@link ScanTableReader} produces and {@link TableWriter} consumes.
 */
public enum ChangeMode {

	INSERT_ONLY(
		Kind.INSERT),

	UPSERT(
		Kind.INSERT,
		Kind.UPDATE_AFTER,
		Kind.DELETE),

	ALL(
		Kind.INSERT,
		Kind.UPDATE_BEFORE,
		Kind.UPDATE_AFTER,
		Kind.DELETE);

	private final Set<Kind> kinds;

	ChangeMode(Kind firstKind, Kind... otherKinds) {
		this.kinds = Collections.unmodifiableSet(EnumSet.of(firstKind, otherKinds));
	}

	public Set<Kind> getSupportedKinds() {
		return kinds;
	}

	public boolean supportsKind(Kind kind) {
		return kinds.contains(kind);
	}
}
