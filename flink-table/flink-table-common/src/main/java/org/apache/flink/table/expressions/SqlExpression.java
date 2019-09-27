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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Column expression that describe the computed column expression.
 * This class holds a STRING format expression so that it can be serialized and persisted.
 *
 * <p>Caution that this class is experimental and the implementation
 * may change in the future.
 */
@Internal
public class SqlExpression implements ResolvedExpression {

	private final String sqlExpression;
	private final DataType type;

	/**
	 * Creates a SQL Expression
	 *
	 * @param sqlExpression SQL expression as STRING format
	 * @param type       derived type of the expression
	 */
	public SqlExpression(String sqlExpression, DataType type) {
		this.sqlExpression = Preconditions.checkNotNull(sqlExpression);
		this.type = type;
	}

	@Override
	public DataType getOutputDataType() {
		return this.type;
	}

	@Override
	public String asSerializableString() {
		return this.sqlExpression;
	}

	@Override
	public List<ResolvedExpression> getResolvedChildren() {
		return Collections.emptyList();
	}

	@Override
	public String asSummaryString() {
		return asSerializableString();
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SqlExpression that = (SqlExpression) o;
		return Objects.equals(sqlExpression, that.sqlExpression) &&
			Objects.equals(type, that.type);
	}

	@Override
	public int hashCode() {
		return Objects.hash(sqlExpression, type);
	}

	@Override
	public String toString() {
		return "SqlExpression{" +
			"sqlExpression='" + sqlExpression + '\'' +
			", type=" + type +
			'}';
	}
}
