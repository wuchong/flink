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

package org.apache.flink.table.dataformat.util;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.ChangelogKind;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Util for base row.
 */
public final class BaseRowUtil {

	public static boolean isAccumulateMsg(BaseRow baseRow) {
		return baseRow.getChangelogKind() == ChangelogKind.INSERT;
	}

	public static boolean isRetractMsg(BaseRow baseRow) {
		return baseRow.getChangelogKind() == ChangelogKind.DELETE;
	}

	public static BaseRow setAccumulate(BaseRow baseRow) {
		baseRow.setChangelogKind(ChangelogKind.INSERT);
		return baseRow;
	}

	public static BaseRow setRetract(BaseRow baseRow) {
		baseRow.setChangelogKind(ChangelogKind.DELETE);
		return baseRow;
	}

	public static GenericRow toGenericRow(
			BaseRow baseRow,
			LogicalType[] types) {
		if (baseRow instanceof GenericRow) {
			return (GenericRow) baseRow;
		} else {
			GenericRow row = new GenericRow(baseRow.getArity());
			row.setChangelogKind(baseRow.getChangelogKind());
			for (int i = 0; i < row.getArity(); i++) {
				if (baseRow.isNullAt(i)) {
					row.setField(i, null);
				} else {
					row.setField(i, BaseRow.get(baseRow, i, types[i]));
				}
			}
			return row;
		}
	}
}
