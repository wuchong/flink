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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql2rel.SqlToRelConverter;

/**
 * Flink extension of {@link RelOptTable.ToRelContext} which contains the
 * context to convert a SQL expression into a {@link org.apache.calcite.rex.RexNode}.
 * This is mainly used for computed column and watermark expression.
 */
public interface FlinkToRelContext extends RelOptTable.ToRelContext {

	/**
	 * Creates a new instance of {@link SqlToRexConverter} to convert SQL expression to RexNode.
	 */
	SqlToRelConverter createSqlToRexConverter(RelDataType tableRowType);
}
