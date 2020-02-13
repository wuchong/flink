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

package org.apache.flink.connectors.jdbc;

import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects;
import org.apache.flink.connectors.jdbc.sink.JDBCTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JDBCValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.TableSinkV2Factory;
import org.apache.flink.table.factories.TableSourceV2Factory;
import org.apache.flink.table.sinks.v2.TableSinkV2;
import org.apache.flink.table.sources.v2.TableSourceV2;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_DRIVER;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_LOOKUP_CACHE_TTL;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_PASSWORD;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_FETCH_SIZE;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_PARTITION_COLUMN;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_PARTITION_LOWER_BOUND;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_PARTITION_NUM;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_READ_PARTITION_UPPER_BOUND;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_TYPE_VALUE_JDBC;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_URL;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_USERNAME;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_WRITE_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_WRITE_FLUSH_MAX_ROWS;
import static org.apache.flink.table.descriptors.JDBCValidator.CONNECTOR_WRITE_MAX_RETRIES;

/**
 * .
 */
public class JDBCTableSourceSinkFactory implements TableSourceV2Factory, TableSinkV2Factory {

	@Override
	public String connectorType() {
		return CONNECTOR_TYPE_VALUE_JDBC;
	}

	@Override
	public Optional<String> connectorVersion() {
		return Optional.empty();
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// common options
		properties.add(CONNECTOR_DRIVER);
		properties.add(CONNECTOR_URL);
		properties.add(CONNECTOR_TABLE);
		properties.add(CONNECTOR_USERNAME);
		properties.add(CONNECTOR_PASSWORD);

		// scan options
		properties.add(CONNECTOR_READ_PARTITION_COLUMN);
		properties.add(CONNECTOR_READ_PARTITION_NUM);
		properties.add(CONNECTOR_READ_PARTITION_LOWER_BOUND);
		properties.add(CONNECTOR_READ_PARTITION_UPPER_BOUND);
		properties.add(CONNECTOR_READ_FETCH_SIZE);

		// lookup options
		properties.add(CONNECTOR_LOOKUP_CACHE_MAX_ROWS);
		properties.add(CONNECTOR_LOOKUP_CACHE_TTL);
		properties.add(CONNECTOR_LOOKUP_MAX_RETRIES);

		// sink options
		properties.add(CONNECTOR_WRITE_FLUSH_MAX_ROWS);
		properties.add(CONNECTOR_WRITE_FLUSH_INTERVAL);
		properties.add(CONNECTOR_WRITE_MAX_RETRIES);

		return properties;
	}

	@Override
	public TableSinkV2 createTableSink(TableSinkV2Factory.Context context) {
		CatalogTable table = context.getTable();
		DescriptorProperties descriptorProperties = getValidatedProperties(table.getProperties());
		TableSchema schema = TableSchemaUtils.getPhysicalSchema(table.getSchema());

		final JDBCTableSink.Builder builder = JDBCTableSink.builder()
			.setOptions(getJDBCOptions(descriptorProperties))
			.setTableSchema(schema);

		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS).ifPresent(builder::setFlushMaxSize);
		descriptorProperties.getOptionalDuration(CONNECTOR_WRITE_FLUSH_INTERVAL).ifPresent(
			s -> builder.setFlushIntervalMills(s.toMillis()));
		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);

		return builder.build();
	}

	@Override
	public TableSourceV2 createTableSource(TableSourceV2Factory.Context context) {
		throw new UnsupportedOperationException();
	}

	// ------------------------------------------------------------------------------

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new SchemaValidator(true, false, false).validate(descriptorProperties);
		new JDBCValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private JDBCOptions getJDBCOptions(DescriptorProperties descriptorProperties) {
		final String url = descriptorProperties.getString(CONNECTOR_URL);
		final JDBCOptions.Builder builder = JDBCOptions.builder()
			.setDBUrl(url)
			.setTableName(descriptorProperties.getString(CONNECTOR_TABLE))
			.setDialect(JDBCDialects.get(url).get());

		descriptorProperties.getOptionalString(CONNECTOR_DRIVER).ifPresent(builder::setDriverName);
		descriptorProperties.getOptionalString(CONNECTOR_USERNAME).ifPresent(builder::setUsername);
		descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).ifPresent(builder::setPassword);

		return builder.build();
	}
}
