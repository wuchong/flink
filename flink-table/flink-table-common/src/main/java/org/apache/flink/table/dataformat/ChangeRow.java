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

package org.apache.flink.table.dataformat;

import org.apache.flink.types.Row;

import java.util.Objects;

/**
 * .
 */
public class ChangeRow {

	private ChangeType changeType;
	private Row row;

	public ChangeRow(ChangeType changeType, Row row) {
		this.changeType = changeType;
		this.row = row;
	}

	public ChangeRow() {
	}

	public ChangeType getChangeType() {
		return changeType;
	}

	public void setChangeType(ChangeType changeType) {
		this.changeType = changeType;
	}

	public Row getRow() {
		return row;
	}

	public void setRow(Row row) {
		this.row = row;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ChangeRow changeRow = (ChangeRow) o;
		return changeType == changeRow.changeType &&
			Objects.equals(row, changeRow.row);
	}

	@Override
	public int hashCode() {
		return Objects.hash(changeType, row);
	}

	@Override
	public String toString() {
		return changeType + " " + row;
	}

	// ------------------------------------------------------------------------------------------

	public static ChangeRow of(Object... values) {
		return new ChangeRow(ChangeType.INSERT, Row.of(values));
	}
}
