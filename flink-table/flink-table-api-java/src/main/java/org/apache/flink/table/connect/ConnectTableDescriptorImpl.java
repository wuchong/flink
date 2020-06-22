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

package org.apache.flink.table.connect;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.Registration;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;

public class ConnectTableDescriptorImpl implements ConnectTableDescriptor {

	private final Parser parser;
	private final Registration registration;

	// Partition keys if this is a partitioned table. It's an empty set if the table is not partitioned
	private final List<String> partitionedFields = new ArrayList<>();

	private final Map<String, String> options = new HashMap<>();

	private TableSchema tableSchema;

	public ConnectTableDescriptorImpl(
			Parser parser,
			Registration registration,
			String connectorIdentifier) {
		this.parser = parser;
		this.registration = registration;
		this.options.put(CONNECTOR.key(), connectorIdentifier);
	}

	public ConnectTableDescriptorImpl(
			Parser parser,
			Registration registration,
			ConnectorDescriptor connectorDescriptor) {
		this.parser = parser;
		this.registration = registration;
		connectorDescriptor.toProperties().forEach(options::put);
	}

	@Override
	public ConnectTableDescriptor schema(Schema schema) {
		this.tableSchema = convertSchemaDescriptorToTableSchema(schema);
		return this;
	}

	@Override
	public ConnectTableDescriptor partitionedBy(String... fieldNames) {
		checkArgument(partitionedFields.isEmpty(), "partitionedBy(...) shouldn't be called more than once.");
		partitionedFields.addAll(Arrays.asList(fieldNames));
		return this;
	}

	@Override
	public ConnectTableDescriptor option(String key, String value) {
		String lowerKey = key.toLowerCase().trim();
		if (CONNECTOR.key().equals(lowerKey)) {
			throw new IllegalArgumentException("It's not allowed to override 'connector' option.");
		}
		options.put(key, value);
		return this;
	}

	@Override
	public ConnectTableDescriptor options(Map<String, String> options) {
		options.forEach(this::option);
		return this;
	}

	@Override
	public void createTemporaryTable(String path) {
		CatalogTable catalogTable = createCatalogTable();
		registration.createTemporaryTable(path, catalogTable);
	}

	@Override
	public Map<String, String> toProperties() {
		return createCatalogTable().toProperties();
	}

	@Override
	public String toString() {
		return DescriptorProperties.toString(toProperties());
	}

	// --------------------------------------------------------------------------------------------

	private CatalogTable createCatalogTable() {
		if (tableSchema == null) {
			throw new TableException("Currently, schema must be explicitly defined.");
		}
		return new CatalogTableImpl(
			tableSchema,
			partitionedFields,
			options,
			null);
	}

	private TableSchema convertSchemaDescriptorToTableSchema(Schema schema) {
		TableSchema.Builder builder = TableSchema.builder();
		// resolve columns
		TableSchema physicalFields = derivePhysicalSchema(schema);
		for (Schema.Column column : schema.columns) {
			if (column instanceof Schema.PhysicalColumn) {
				String fieldName = column.name();
				// get field type from physicalFields which has correct nullable information already
				DataType fieldType = physicalFields.getFieldDataType(fieldName).get();
				builder.field(fieldName, fieldType);
			} else if (column instanceof Schema.VirtualColumn) {
				String expression = ((Schema.VirtualColumn) column).expression();
				DataType columnType = resolveExpressionDataType(
					expression,
					physicalFields);
				builder.field(column.name(), columnType, expression);
			} else {
				throw new UnsupportedOperationException(
					"Unsupported column type: " + column.getClass().getSimpleName());
			}
		}

		// resolve watermark
		TableSchema allFields = builder.build();
		Schema.WatermarkInfo watermarkInfo = schema.watermarkInfo;
		if (watermarkInfo != null) {
			String rowtime = watermarkInfo.rowtimeAttribute();
			long delayMillis = deriveWatermarkDelayMillis(watermarkInfo);
			String delay = String.valueOf(delayMillis / 1000.0);
			String wmExpr = String.format("`%s` - INTERVAL '%s' SECOND", rowtime, delay);
			DataType wmOutputType = resolveExpressionDataType(wmExpr, allFields);
			builder.watermark(watermarkInfo.rowtimeAttribute(), wmExpr, wmOutputType);
		}

		// resolve primary key
		List<String> primaryKey = schema.primaryKey;
		if (primaryKey != null) {
			builder.primaryKey(primaryKey.toArray(new String[0]));
		}
		return builder.build();
	}

	private TableSchema derivePhysicalSchema(Schema schema) {
		TableSchema.Builder builder = TableSchema.builder();
		for (Schema.Column column : schema.columns) {
			if (column instanceof Schema.PhysicalColumn) {
				DataType fieldType = ((Schema.PhysicalColumn) column).type();
				String fieldName = column.name();
				// convert field type to NOT NULL if it is primary key.
				fieldType = isPrimaryKeyField(fieldName, schema) ? fieldType.notNull() : fieldType;
				builder.field(fieldName, fieldType);
			}
		}
		return builder.build();
	}

	private boolean isPrimaryKeyField(String fieldName, Schema schema) {
		List<String> primaryKey = schema.primaryKey;
		if (primaryKey != null) {
			return primaryKey.contains(fieldName);
		}
		return false;
	}

	private DataType resolveExpressionDataType(String expr, TableSchema tableSchema) {
		ResolvedExpression resolvedExpr = parser.parseSqlExpression(expr, tableSchema);
		if (resolvedExpr == null) {
			throw new ValidationException("Could not resolve field expression: " + expr);
		}
		return resolvedExpr.getOutputDataType();
	}

	private long deriveWatermarkDelayMillis(Schema.WatermarkInfo watermarkInfo) {
		if (watermarkInfo instanceof Schema.AscendingTimestamps) {
			return 1;
		} else if (watermarkInfo instanceof Schema.BoundedOutOfOrderTimestamps) {
			return ((Schema.BoundedOutOfOrderTimestamps) watermarkInfo)
				.boundedOutOfOrderness()
				.toMillis();
		} else {
			throw new UnsupportedOperationException(
				"Unsupported watermark type: " + watermarkInfo.getClass().getSimpleName());
		}
	}
}
