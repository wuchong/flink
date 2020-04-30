/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.jdbc;

import org.apache.flink.connectors.jdbc.dialect.JdbcDialect;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

abstract class JdbcTypedQueryOptions implements Serializable {

	private final JdbcDialect dialect;
	@Nullable
	private final JdbcDataType[] fieldTypes;

	public JdbcTypedQueryOptions(JdbcDialect dialect, @Nullable JdbcDataType[] fieldTypes) {
		this.dialect = Preconditions.checkNotNull(dialect, "dialect name is empty");
		this.fieldTypes = fieldTypes;
	}

	public JdbcDialect getDialect() {
		return dialect;
	}

	public JdbcDataType[] getFieldTypes() {
		return fieldTypes;
	}

	public abstract static class JDBCUpdateQueryOptionsBuilder<T extends JDBCUpdateQueryOptionsBuilder<T>> {
		JdbcDataType[] fieldTypes;

		protected abstract T self();

		T withFieldTypes(JdbcDataType[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return self();
		}
	}
}
