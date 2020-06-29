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

package org.apache.flink.table.descriptor;

import java.util.HashMap;
import java.util.Map;

/**
  * Format descriptor for JSON.
  */
public class Json extends FormatDescriptor {

	private Boolean failOnMissingField;
	private Boolean deriveSchema;
	private Boolean ignoreParseErrors;
	private String jsonSchema;
	private String schema;

	/**
	  * Format descriptor for JSON.
	  */
	public Json() {
	}

	/**
	 * Sets flag whether to fail if a field is missing or not.
	 *
	 * @param failOnMissingField If set to true, the operation fails if there is a missing field.
	 *                           If set to false, a missing field is set to null.
	 */
	public Json failOnMissingField(boolean failOnMissingField) {
		this.failOnMissingField = failOnMissingField;
		return this;
	}

	/**
	 * Sets flag whether to fail when parsing json fails.
	 *
	 * @param ignoreParseErrors If set to true, the operation will ignore parse errors.
	 *                          If set to false, the operation fails when parsing json fails.
	 */
	public Json ignoreParseErrors(boolean ignoreParseErrors) {
		this.ignoreParseErrors = ignoreParseErrors;
		return this;
	}

	@Override
	protected Map<String, String> toFormatOptions() {
		return new HashMap<>();
	}
}
