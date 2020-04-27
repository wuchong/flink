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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.jdbc.JdbcLookupFunction;
import org.apache.flink.connectors.jdbc.JdbcLookupOptions;
import org.apache.flink.connectors.jdbc.JdbcOptions;
import org.apache.flink.table.functions.TableFunction;

/**
 * A {@link TableFunction} to query fields from JDBC by keys.
 * The query template like:
 * <PRE>
 * SELECT c, d, e, f from T where a = ? and b = ?
 * </PRE>
 *
 * <p>Support cache the result to avoid frequent accessing to remote databases.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 */
@Deprecated
public class JDBCLookupFunction extends JdbcLookupFunction {
	public JDBCLookupFunction(JdbcOptions options, JdbcLookupOptions lookupOptions, String[] fieldNames, TypeInformation[] fieldTypes, String[] keyNames) {
		super(options, lookupOptions, fieldNames, fieldTypes, keyNames);
	}
}
