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

package org.apache.flink.table.api;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TableEnvironment}.
 */
public class TableEnvironmentTest {

//	@Test
//	public void testConnect() {
//		final TableEnvironmentMock tableEnv = TableEnvironmentMock.getStreamingInstance();
//
//		tableEnv
//			.connect(new ConnectorDescriptorMock(TableSourceFactoryMock.CONNECTOR_TYPE_VALUE, 1, true))
//			.withFormat(new FormatDescriptorMock("my_format", 1))
//			.withSchema(new Schema()
//				.field("my_field_0", "INT")
//				.field("my_field_1", "BOOLEAN")
//				.field("my_part_1", "BIGINT")
//				.field("my_part_2", "STRING"))
//			.withPartitionKeys(Arrays.asList("my_part_1", "my_part_2"))
//			.inAppendMode()
//			.createTemporaryTable("my_table");
//
//		CatalogManager.TableLookupResult lookupResult = tableEnv.catalogManager.getTable(ObjectIdentifier.of(
//			EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
//			EnvironmentSettings.DEFAULT_BUILTIN_DATABASE,
//			"my_table"))
//			.orElseThrow(AssertionError::new);
//
//		assertThat(lookupResult.isTemporary(), equalTo(true));
//
//		CatalogBaseTable catalogBaseTable = lookupResult.getTable();
//		assertTrue(catalogBaseTable instanceof CatalogTable);
//		CatalogTable table = (CatalogTable) catalogBaseTable;
//		assertCatalogTable(table);
//		assertCatalogTable(CatalogTableImpl.fromProperties(table.toProperties()));
//	}

	@Test
	public void testConnectIdentifier() {
		final TableEnvironmentMock tableEnv = TableEnvironmentMock.getStreamingInstance();

		tableEnv.connect("values")
			.schema(new org.apache.flink.table.connect.Schema()
				.column("my_field_0", DataTypes.INT())
				.column("my_field_1", DataTypes.BOOLEAN())
				.column("my_field_2", DataTypes.DECIMAL(10, 2))
				.columnProctime("my_proctime")
				.column("my_field_3", DataTypes.TIMESTAMP(3))
				.column("my_part_1", DataTypes.STRING())
				.column("my_part_2", DataTypes.BIGINT())
				.watermarkFor("my_field_3").boundedOutOfOrderTimestamps(Duration.ofMillis(2))
				.primaryKey("my_field_1", "my_field_0"))
			.partitionedBy("my_part_1", "my_part_2")
			.option("bounded", "false")
			.option("async", "true")
			.createTemporaryTable("my_table");

		CatalogManager.TableLookupResult lookupResult = tableEnv.catalogManager.getTable(ObjectIdentifier.of(
			EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
			EnvironmentSettings.DEFAULT_BUILTIN_DATABASE,
			"my_table"))
			.orElseThrow(AssertionError::new);

		assertThat(lookupResult.isTemporary(), equalTo(true));

		CatalogBaseTable catalogBaseTable = lookupResult.getTable();
		assertTrue(catalogBaseTable instanceof CatalogTable);
		CatalogTable table = (CatalogTable) catalogBaseTable;
		assertCatalogTable(table);
		assertCatalogTable(CatalogTableImpl.fromProperties(table.toProperties()));
	}

	private static void assertCatalogTable(CatalogTable table) {
		assertThat(
				table.getSchema(),
				equalTo(
						TableSchema.builder()
								.field("my_field_0", DataTypes.INT())
								.field("my_field_1", DataTypes.BOOLEAN())
								.field("my_field_2", DataTypes.DECIMAL(10, 2))
								.field("my_proctime", DataTypes.TIMESTAMP(3), "PROCTIME()")
								.field("my_field_3", DataTypes.TIMESTAMP(3))
								.field("my_part_1", DataTypes.STRING())
								.field("my_part_2", DataTypes.BIGINT())
								.watermark("my_field_3", "`my_field_3` - INTERVAL '0.002' SECOND", DataTypes.TIMESTAMP(3))
								.primaryKey("my_field_1", "my_field_0")
								.build()));
		assertThat(
				table.getPartitionKeys(),
				equalTo(Arrays.asList("my_part_1", "my_part_2")));

		Map<String, String> options = new HashMap<>();
		options.put("connector", "values");
		options.put("bounded", "false");
		options.put("async", "true");
		assertThat(table.getOptions(), equalTo(options));
	}
}
