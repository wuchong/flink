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

package org.apache.flink.addons.hbase;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.DynamicTableSink;
import org.apache.flink.table.connectors.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.HBaseValidator;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ZK_NODE_PARENT;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link HBaseDynamicTableSink}.
 */
public class HBaseDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	@Override
	public DynamicTableSource createTableSource(DynamicTableSourceFactory.Context context) {
		Map<String, String> properties = context.getTable().getProperties();
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		Configuration hbaseClientConf = HBaseConfiguration.create();
		String hbaseZk = descriptorProperties.getString(CONNECTOR_ZK_QUORUM);
		hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, hbaseZk);
		descriptorProperties
			.getOptionalString(CONNECTOR_ZK_NODE_PARENT)
			.ifPresent(v -> hbaseClientConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, v));

		String hTableName = descriptorProperties.getString(CONNECTOR_TABLE_NAME);
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema());
		return new HBaseDynamicTableSource(hbaseClientConf, tableSchema, hTableName);
	}

	@Override
	public DynamicTableSink createTableSink(DynamicTableSinkFactory.Context context) {
		Map<String, String> properties = context.getTable().getProperties();
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		HBaseOptions.Builder hbaseOptionsBuilder = HBaseOptions.builder();
		hbaseOptionsBuilder.setZkQuorum(descriptorProperties.getString(CONNECTOR_ZK_QUORUM));
		hbaseOptionsBuilder.setTableName(descriptorProperties.getString(CONNECTOR_TABLE_NAME));
		descriptorProperties
			.getOptionalString(CONNECTOR_ZK_NODE_PARENT)
			.ifPresent(hbaseOptionsBuilder::setZkNodeParent);

		TableSchema tableSchema = context.getTable().getSchema();
		HBaseTableSchema hbaseSchema = HBaseTableSchema.build(tableSchema);

		HBaseWriteOptions.Builder writeBuilder = HBaseWriteOptions.builder();
		descriptorProperties
			.getOptionalInt(CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS)
			.ifPresent(writeBuilder::setBufferFlushMaxRows);
		descriptorProperties
			.getOptionalMemorySize(CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE)
			.ifPresent(v -> writeBuilder.setBufferFlushMaxSizeInBytes(v.getBytes()));
		descriptorProperties
			.getOptionalDuration(CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL)
			.ifPresent(v -> writeBuilder.setBufferFlushIntervalMillis(v.toMillis()));

		return new HBaseDynamicTableSink(
			hbaseSchema,
			hbaseOptionsBuilder.build(),
			writeBuilder.build()
		);
	}


	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE); // hbase
		context.put(CONNECTOR_VERSION, CONNECTOR_VERSION_VALUE_143); // version
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(CONNECTOR_TABLE_NAME);
		properties.add(CONNECTOR_ZK_QUORUM);
		properties.add(CONNECTOR_ZK_NODE_PARENT);
		properties.add(CONNECTOR_WRITE_BUFFER_FLUSH_MAX_SIZE);
		properties.add(CONNECTOR_WRITE_BUFFER_FLUSH_MAX_ROWS);
		properties.add(CONNECTOR_WRITE_BUFFER_FLUSH_INTERVAL);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		// computed column
		properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);

		// watermark
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);

		return properties;
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new HBaseValidator().validate(descriptorProperties);
		return descriptorProperties;
	}
}
