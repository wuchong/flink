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

import org.apache.flink.connectors.jdbc.JdbcLookupOptions;

/**
 * Options for the JDBC lookup.
 *
 * @deprecated Please use {@link JdbcLookupOptions}, Flink proposes class name start with "Jdbc" rather than "JDBC".
 */
@Deprecated
public class JDBCLookupOptions extends JdbcLookupOptions {

	protected JDBCLookupOptions(long cacheMaxSize, long cacheExpireMs, int maxRetryTimes) {
		super(cacheMaxSize, cacheExpireMs, maxRetryTimes);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder of {@link JDBCLookupOptions}.
	 */
	public static class Builder extends JdbcLookupOptions.Builder {

		@Override
		public Builder setCacheMaxSize(long cacheMaxSize) {
			return (Builder) super.setCacheMaxSize(cacheMaxSize);
		}

		@Override
		public Builder setCacheExpireMs(long cacheExpireMs) {
			return (Builder) super.setCacheExpireMs(cacheExpireMs);
		}

		@Override
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			return (Builder) super.setMaxRetryTimes(maxRetryTimes);
		}

		@Override
		public JDBCLookupOptions build() {
			return new JDBCLookupOptions(cacheMaxSize, cacheExpireMs, maxRetryTimes);
		}
	}
}
