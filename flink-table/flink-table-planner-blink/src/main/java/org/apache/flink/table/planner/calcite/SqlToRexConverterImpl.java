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

package org.apache.flink.table.planner.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Standard implementation of {@link SqlToRexConverter}.
 */
public class SqlToRexConverterImpl implements SqlToRexConverter {

	private static final String TEMPORARY_TABLE_NAME = "__temp_table__";
	private static final String QUERY_FORMAT = "SELECT %s FROM " + TEMPORARY_TABLE_NAME;

	private final FlinkPlannerImpl planner;

	public SqlToRexConverterImpl(
			FrameworkConfig config,
			FlinkTypeFactory typeFactory,
			RelOptCluster cluster,
			RelDataType tableRowType) {
		this.planner = new FlinkPlannerImpl(
			config,
			(isLenient) -> createSingleTableCatalogReader(isLenient, config, typeFactory, tableRowType),
			typeFactory,
			cluster
		);
	}

	@Override
	public RexNode convertToRexNode(String expr) {
		return convertToRexNodes(new String[]{expr})[0];
	}

	@Override
	public RexNode[] convertToRexNodes(String[] exprs) {
		String query = String.format(QUERY_FORMAT, String.join(",", exprs));
		SqlNode parsed = planner.parse(query);
		SqlNode validated = planner.validate(parsed);
		RelNode rel = planner.rel(validated).rel;
		if (rel instanceof LogicalProject) {
			return ((LogicalProject) rel).getProjects().toArray(new RexNode[0]);
		} else {
			throw new IllegalStateException("The root RelNode should be LogicalProject, but is " + rel.toString());
		}
	}

	// ------------------------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------------------------

	/**
	 * Creates a catalog reader that contains a single {@link Table} with temporary table name
	 * and specified {@code rowType}.
	 *
	 * @param rowType     table row type
	 * @return the {@link CalciteCatalogReader} instance
	 */
	private static CalciteCatalogReader createSingleTableCatalogReader(
			boolean lenientCaseSensitivity,
			FrameworkConfig config,
			FlinkTypeFactory typeFactory,
			RelDataType rowType) {

		// connection properties
		boolean caseSensitive = !lenientCaseSensitivity && config.getParserConfig().caseSensitive();
		Properties properties = new Properties();
		properties.put(
			CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
			String.valueOf(caseSensitive));
		CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);

		// prepare root schema
		final ExplicitRowTypeTable table = new ExplicitRowTypeTable(rowType);
		final Map<String, Table> tableMap = Collections.singletonMap(TEMPORARY_TABLE_NAME, table);
		CalciteSchema schema = CalciteSchemaBuilder.asRootSchema(new ExplicitTablesSchema(tableMap));

		return new FlinkCalciteCatalogReader(
			schema,
			new ArrayList<>(new ArrayList<>()),
			typeFactory,
			connectionConfig);
	}

	// ------------------------------------------------------------------------------------------
	// Inner Class
	// ------------------------------------------------------------------------------------------

	/**
	 * A {@link AbstractTable} that can specify the row type explicitly.
	 */
	private static class ExplicitRowTypeTable extends AbstractTable {
		private final RelDataType rowType;

		ExplicitRowTypeTable(RelDataType rowType) {
			this.rowType = Objects.requireNonNull(rowType);
		}

		@Override
		public RelDataType getRowType(RelDataTypeFactory typeFactory) {
			return this.rowType;
		}
	}

	/**
	 * A {@link AbstractSchema} that can specify the table map explicitly.
	 */
	private static class ExplicitTablesSchema extends AbstractSchema {
		private final Map<String, Table> tableMap;

		ExplicitTablesSchema(Map<String, Table> tableMap) {
			this.tableMap = Objects.requireNonNull(tableMap);
		}

		@Override
		protected Map<String, Table> getTableMap() {
			return tableMap;
		}
	}
}
