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

package org.apache.flink.connectors.jdbc.dialect;

import java.io.Serializable;

/**
 * The class to describe a JDBC data type.
 * For example:
 * JdbcType jdbcType = new JdbcType("DOUBLE", java.sql.Types.DOUBLE) can describe a "DOUBLE" type.
 * the dialectTypeName "DOUBLE" describe the dialect type name in a database,
 * the genericSqlType "java.sql.Types.DOUBLE" describes corresponding generic type
 * which should be a integer constant from java.sql.Types.
 */
public class JdbcType implements Serializable {

	private static final long serialVersionUID = 1L;
	private final String dialectTypeName;
	private final int genericSqlType;

	public JdbcType(String dialectTypeName, int genericSqlType) {
		this.dialectTypeName = dialectTypeName;
		this.genericSqlType = genericSqlType;
	}

	public String getDialectTypeName() {
		return dialectTypeName;
	}

	public int getGenericSqlType() {
		return genericSqlType;
	}
}
