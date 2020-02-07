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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.table.dataformat.ChangeRow;

import java.util.List;

/**
 * .
 */
public class ChangeRowTypeInfo extends CompositeType<ChangeRow> {

	private RowTypeInfo rowTypeInfo;

	public ChangeRowTypeInfo(RowTypeInfo rowTypeInfo) {
		super(ChangeRow.class);
		this.rowTypeInfo = rowTypeInfo;
	}

	public ChangeRowTypeInfo(TypeInformation<?>... types) {
		this(new RowTypeInfo(types));
	}

	@Override
	public void getFlatFields(String fieldExpression, int offset, List<FlatFieldDescriptor> result) {
		rowTypeInfo.getFlatFields(fieldExpression, offset, result);
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {
		return rowTypeInfo.getTypeAt(fieldExpression);
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(int pos) {
		return rowTypeInfo.getTypeAt(pos);
	}

	@Override
	protected TypeComparatorBuilder<ChangeRow> createTypeComparatorBuilder() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String[] getFieldNames() {
		return rowTypeInfo.getFieldNames();
	}

	@Override
	public int getFieldIndex(String fieldName) {
		return rowTypeInfo.getFieldIndex(fieldName);
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return rowTypeInfo.getArity();
	}

	@Override
	public int getTotalFields() {
		return rowTypeInfo.getTotalFields();
	}

	@Override
	public TypeSerializer<ChangeRow> createSerializer(ExecutionConfig config) {
		return new ChangeRowSerializer((RowSerializer) rowTypeInfo.createSerializer(config));
	}

	@Override
	public boolean hasDeterministicFieldOrder() {
		return true;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ChangeRowTypeInfo;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ChangeRowTypeInfo) {
			ChangeRowTypeInfo other = (ChangeRowTypeInfo) obj;
			return this.rowTypeInfo.equals(other.rowTypeInfo);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return 31 + rowTypeInfo.hashCode();
	}

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder("ChangeRow");
		TypeInformation<?>[] types = rowTypeInfo.getFieldTypes();
		String[] fieldNames = rowTypeInfo.getFieldNames();
		if (types.length > 0) {
			bld.append('(').append(fieldNames[0]).append(": ").append(types[0]);

			for (int i = 1; i < types.length; i++) {
				bld.append(", ").append(fieldNames[i]).append(": ").append(types[i]);
			}

			bld.append(')');
		}
		return bld.toString();
	}
}
