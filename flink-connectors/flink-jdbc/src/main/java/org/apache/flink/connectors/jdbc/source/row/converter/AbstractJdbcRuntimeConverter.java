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

package org.apache.flink.connectors.jdbc.source.row.converter;

import org.apache.flink.connectors.jdbc.JdbcDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base for all row converters.
 */
public abstract class AbstractJdbcRuntimeConverter implements JdbcRuntimeConverter {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcRuntimeConverter.class);
	protected final RowType rowType;
	protected final JDBCFieldConverter[] toInternalConverters;

	public AbstractJdbcRuntimeConverter(RowType rowType) {
		this.rowType = checkNotNull(rowType);

		toInternalConverters = new JDBCFieldConverter[rowType.getFieldCount()];
		for (int i = 0; i < toInternalConverters.length; i++) {
			toInternalConverters[i] = createInternalConverter(rowType.getTypeAt(i));
		}
	}

	@Override
	public Row toRowData(ResultSet externalData, Row reuse) throws SQLException {
		for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
			reuse.setField(pos, toInternalConverters[pos].convert(externalData.getObject(pos + 1)));
		}
		return reuse;
	}

	/**
	 * Create a runtime JDBC field converter from given {@link LogicalType}.
	 */
	public JDBCFieldConverter createInternalConverter(LogicalType type) {
		LogicalTypeRoot root = type.getTypeRoot();

		if (root == LogicalTypeRoot.SMALLINT) {
			// Converter for small type that casts value to int and then return short value, since
	        // JDBC 1.0 use int type for small values.
			return v -> ((Integer) v).shortValue();
		} else {
			return v -> v;
		}
	}

	/**
	 * Create a runtime JDBC field converter from given {@link JdbcDataType}.
	 */
	public JDBCFieldConverter createExternalConverter(int sqlType) {
		return field -> {
			if (field == null) {
				return field;
			} else {
				switch (sqlType) {
					case java.sql.Types.NULL:
						return null;
					case java.sql.Types.BOOLEAN:
					case java.sql.Types.BIT:
						return (boolean) field;
					case java.sql.Types.CHAR:
					case java.sql.Types.NCHAR:
					case java.sql.Types.VARCHAR:
					case java.sql.Types.LONGVARCHAR:
					case java.sql.Types.LONGNVARCHAR:
						return (String) field;
					case java.sql.Types.TINYINT:
						return (byte) field;
					case java.sql.Types.SMALLINT:
						return (short) field;
					case java.sql.Types.INTEGER:
						return (int) field;
					case java.sql.Types.BIGINT:
						return (long) field;
					case java.sql.Types.REAL:
						return (float) field;
					case java.sql.Types.FLOAT:
					case java.sql.Types.DOUBLE:
						return (double) field;
					case java.sql.Types.DECIMAL:
					case java.sql.Types.NUMERIC:
						return (java.math.BigDecimal) field;
					case java.sql.Types.DATE:
						return (java.sql.Date) field;
					case java.sql.Types.TIME:
						return (java.sql.Time) field;
					case java.sql.Types.TIMESTAMP:
						return (java.sql.Timestamp) field;
					case java.sql.Types.BINARY:
					case java.sql.Types.VARBINARY:
					case java.sql.Types.LONGVARBINARY:
						return (byte[]) field;
					default:
						return field;
				}
			}
		};
	}
}
