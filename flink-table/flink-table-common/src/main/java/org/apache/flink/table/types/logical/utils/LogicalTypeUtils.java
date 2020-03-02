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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformats.BaseArray;
import org.apache.flink.table.dataformats.BaseMap;
import org.apache.flink.table.dataformats.BaseRow;
import org.apache.flink.table.dataformats.BinaryGeneric;
import org.apache.flink.table.dataformats.BinaryString;
import org.apache.flink.table.dataformats.Decimal;
import org.apache.flink.table.dataformats.SqlTimestamp;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

/**
 * Utilities for handling {@link LogicalType}s.
 */
@Internal
public final class LogicalTypeUtils {

	private static final TimeAttributeRemover TIME_ATTRIBUTE_REMOVER = new TimeAttributeRemover();

	public static LogicalType removeTimeAttributes(LogicalType logicalType) {
		return logicalType.accept(TIME_ATTRIBUTE_REMOVER);
	}

	/**
	 * Get internal(sql engine execution data formats) conversion class for {@link LogicalType}.
	 */
	public static Class<?> internalConversionClass(LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return Boolean.class;
			case TINYINT:
				return Byte.class;
			case SMALLINT:
				return Short.class;
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return Integer.class;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return Long.class;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return SqlTimestamp.class;
			case FLOAT:
				return Float.class;
			case DOUBLE:
				return Double.class;
			case CHAR:
			case VARCHAR:
				return BinaryString.class;
			case DECIMAL:
				return Decimal.class;
			case ARRAY:
				return BaseArray.class;
			case MAP:
			case MULTISET:
				return BaseMap.class;
			case ROW:
				return BaseRow.class;
			case BINARY:
			case VARBINARY:
				return byte[].class;
			case RAW:
				return BinaryGeneric.class;
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}

	/**
	 * Get internal(sql engine execution data formats) {@link TypeSerializer} for {@link LogicalType}.
	 */
	public static TypeSerializer<?> internalTypeSerializer(LogicalType type, ExecutionConfig conf) {
		return internalTypeInfo(type).createSerializer(conf);
	}

	public static TypeInformation<?> internalTypeInfo(LogicalType type) {
		return null;
	}

	// --------------------------------------------------------------------------------------------

	private static class TimeAttributeRemover extends LogicalTypeDuplicator {

		@Override
		public LogicalType visit(TimestampType timestampType) {
			return new TimestampType(
				timestampType.isNullable(),
				timestampType.getPrecision());
		}

		@Override
		public LogicalType visit(ZonedTimestampType zonedTimestampType) {
			return new ZonedTimestampType(
				zonedTimestampType.isNullable(),
				zonedTimestampType.getPrecision());
		}

		@Override
		public LogicalType visit(LocalZonedTimestampType localZonedTimestampType) {
			return new LocalZonedTimestampType(
				localZonedTimestampType.isNullable(),
				localZonedTimestampType.getPrecision());
		}
	}

	private LogicalTypeUtils() {
		// no instantiation
	}
}
