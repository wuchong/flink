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

package org.apache.flink.table.descriptor2;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Schema {

	List<Column> columns = new ArrayList<>();
	@Nullable WatermarkInfo watermarkInfo;
	@Nullable List<String> primaryKey;

	public Schema column(String fieldName, DataType fieldType) {
		columns.add(new PhysicalColumn(fieldName, fieldType));
		return this;
	}

	public Schema computedColumn(String fieldName, String sqlExpression) {
		columns.add(new VirtualColumn(fieldName, sqlExpression));
		return this;
	}

	public Schema proctime(String fieldName) {
		this.computedColumn(fieldName, "PROCTIME()");
		return this;
	}

	public Schema primaryKey(String... fieldNames) {
		if (this.primaryKey != null) {
			throw new ValidationException("Can not create multiple PRIMARY keys.");
		}
		if (fieldNames == null || fieldNames.length == 0) {
			throw new ValidationException("PRIMARY KEY constraint must be defined for at least a single column.");
		}
		this.primaryKey = Arrays.asList(fieldNames);
		return this;
	}

	public SchemaWithWatermark watermarkFor(String rowtimeField) {
		return new SchemaWithWatermark(this, rowtimeField);
	}

	public static class SchemaWithWatermark {

		private final Schema schema;
		private final String rowtimeAttribute;

		public SchemaWithWatermark(Schema schema, String rowtimeField) {
			this.schema = schema;
			this.rowtimeAttribute = rowtimeField;
		}

		/**
		 * Creates a watermark strategy for situations with monotonously ascending timestamps.
		 *
		 * <p>The watermarks are generated periodically and tightly follow the latest
		 * timestamp in the data. The delay introduced by this strategy is mainly the periodic interval
		 * in which the watermarks are generated.
		 *
		 * @see AscendingTimestampsWatermarks
		 */
		public Schema ascendingTimestamps() {
			schema.watermarkInfo = new AscendingTimestamps(rowtimeAttribute);
			return schema;
		}

		/**
		 * Creates a watermark strategy for situations where records are out of order, but you can place
		 * an upper bound on how far the events are out of order. An out-of-order bound B means that
		 * once the an event with timestamp T was encountered, no events older than {@code T - B} will
		 * follow any more.
		 *
		 * <p>The watermarks are generated periodically. The delay introduced by this watermark
		 * strategy is the periodic interval length, plus the out of orderness bound.
		 *
		 * @see BoundedOutOfOrdernessWatermarks
		 */
		public Schema boundedOutOfOrderTimestamps(Duration maxOutOfOrderness) {
			schema.watermarkInfo = new BoundedOutOfOrderTimestamps(rowtimeAttribute, maxOutOfOrderness);
			return schema;
		}
	}

	interface Column {
		String name();
	}

	interface WatermarkInfo {
		String rowtimeAttribute();
	}

	static class AscendingTimestamps implements WatermarkInfo {
		private final String rowtimeAttribute;

		AscendingTimestamps(String rowtimeAttribute) {
			this.rowtimeAttribute = rowtimeAttribute;
		}

		@Override
		public String rowtimeAttribute() {
			return rowtimeAttribute;
		}
	}

	static class BoundedOutOfOrderTimestamps implements WatermarkInfo {

		private final String rowtimeAttribute;
		private final Duration maxOutOfOrderness;

		BoundedOutOfOrderTimestamps(String rowtimeAttribute, Duration maxOutOfOrderness) {
			this.rowtimeAttribute = rowtimeAttribute;
			this.maxOutOfOrderness = maxOutOfOrderness;
		}

		public Duration boundedOutOfOrderness() {
			return maxOutOfOrderness;
		}

		@Override
		public String rowtimeAttribute() {
			return rowtimeAttribute;
		}
	}

	static class PhysicalColumn implements Column {

		private final String name;
		private final DataType type;

		PhysicalColumn(String name, DataType type) {
			this.name = name;
			this.type = type;
		}

		@Override
		public String name() {
			return name;
		}

		public DataType type() {
			return type;
		}
	}

	static class VirtualColumn implements Column {

		private final String name;
		private final String expression;

		VirtualColumn(String name, String expression) {
			this.name = name;
			this.expression = expression;
		}

		@Override
		public String name() {
			return name;
		}

		public String expression() {
			return expression;
		}
	}
}
