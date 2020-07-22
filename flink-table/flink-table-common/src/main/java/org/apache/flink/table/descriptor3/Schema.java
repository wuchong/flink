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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.AbstractDataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Schema {

	// package visible
	final List<Column> columns;
	final WatermarkInfo watermarkInfo;
	final List<String> primaryKey;

	private Schema(List<Column> columns, WatermarkInfo watermarkInfo, List<String> primaryKey) {
		this.columns = columns;
		this.watermarkInfo = watermarkInfo;
		this.primaryKey = primaryKey;
	}

	public static SchemaBuilder newBuilder() {
		return new SchemaBuilder();
	}

	public static class SchemaBuilder {
		List<Column> columns = new ArrayList<>();
		@Nullable WatermarkInfo watermarkInfo;
		@Nullable List<String> primaryKey;

		private SchemaBuilder() {
		}

		/**
		 * Adds a column with the column name and the data type.
		 */
		public SchemaBuilder column(String fieldName, AbstractDataType<?> fieldType) {
			columns.add(new PhysicalColumn(fieldName, fieldType));
			return this;
		}

		public SchemaBuilder column(String fieldName, Expression expr) {
			columns.add(new VirtualColumn(fieldName, expr));
			return this;
		}

		public SchemaBuilder primaryKey(String... fieldNames) {
			if (this.primaryKey != null) {
				throw new ValidationException("Can not create multiple PRIMARY keys.");
			}
			if (fieldNames == null || fieldNames.length == 0) {
				throw new ValidationException("PRIMARY KEY constraint must be defined for at least a single column.");
			}
			this.primaryKey = Arrays.asList(fieldNames);
			return this;
		}

		public SchemaBuilder watermark(String rowtimeField, Expression watermarkExpr) {
			this.watermarkInfo = new WatermarkInfo(rowtimeField, watermarkExpr);
			return this;
		}

		public Schema build() {
			return new Schema(columns, watermarkInfo, primaryKey);
		}
	}

	// ------------------------------------------------------------------------------------

	interface Column {
		String name();
	}

	static class WatermarkInfo {
		private final String rowtimeAttribute;
		private final Expression expression;

		WatermarkInfo(String rowtimeAttribute, Expression expression) {
			this.rowtimeAttribute = rowtimeAttribute;
			this.expression = expression;
		}
	}

	static class PhysicalColumn implements Column {

		private final String name;
		private final AbstractDataType<?> type;

		PhysicalColumn(String name, AbstractDataType<?> type) {
			this.name = name;
			this.type = type;
		}

		@Override
		public String name() {
			return name;
		}

		public AbstractDataType<?> type() {
			return type;
		}
	}

	static class VirtualColumn implements Column {

		private final String name;
		private final Expression expression;

		VirtualColumn(String name, Expression expression) {
			this.name = name;
			this.expression = expression;
		}

		@Override
		public String name() {
			return name;
		}

		public Expression expression() {
			return expression;
		}
	}
}
