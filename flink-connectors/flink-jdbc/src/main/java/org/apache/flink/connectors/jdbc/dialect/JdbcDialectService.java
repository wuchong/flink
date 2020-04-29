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

import org.apache.flink.table.api.TableException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Util class to find {@link JdbcDialect} from supported JDBC url.
 */
public class JdbcDialectService {

	/**
	 * Fetch the JdbcDialect class corresponding to a given database url.
	 */
	public static Optional<JdbcDialect> get(String url) {
		try {
			List<JdbcDialect> dialects = new ArrayList<>();
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			ServiceLoader
				.load(JdbcDialect.class, classLoader)
				.iterator()
				.forEachRemaining(dialects::add);
			for (JdbcDialect dialect : dialects) {
				if (dialect.canHandle(url)) {
					return Optional.of(dialect);
				}
			}
		} catch (ServiceConfigurationError e) {
			throw new TableException("Could not load service provider for dialect.", e);
		}
		return Optional.empty();
	}
}
