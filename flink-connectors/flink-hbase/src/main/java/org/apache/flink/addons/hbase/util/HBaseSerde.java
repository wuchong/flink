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

package org.apache.flink.addons.hbase.util;

import org.apache.flink.addons.hbase.HBaseTableSchema;
import org.apache.flink.table.datastructures.RowData;
import org.apache.flink.table.datastructures.DecimalData;
import org.apache.flink.table.datastructures.GenericRowData;
import org.apache.flink.table.datastructures.StringData;
import org.apache.flink.table.datastructures.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

public class HBaseSerde {
	// row key index in output row
	private final int rowkeyIndex;
	private final LogicalType rowkeyType;

	// family keys
	private final byte[][] families;
	// qualifier keys
	private final byte[][][] qualifiers;
	// qualifier types
	private final LogicalType[][] qualifierTypes;

	private final int fieldLength;

	private GenericRowData reusedRow;
	private GenericRowData[] reusedFamilyRows;
	
	public HBaseSerde(HBaseTableSchema hbaseSchema) {
		this.families = hbaseSchema.getFamilyKeys();
		this.rowkeyIndex = hbaseSchema.getRowKeyIndex();
		this.rowkeyType = hbaseSchema.getRowKeyDataType().map(DataType::getLogicalType).orElse(null);

		// field length need take row key into account if it exists.
		checkArgument(rowkeyIndex != -1, "row key shouldn't be null.");
		this.fieldLength = families.length + 1;

		// prepare output rows
		this.reusedRow = new GenericRowData(fieldLength);
		this.reusedFamilyRows = new GenericRowData[families.length];

		this.qualifiers = new byte[families.length][][];
		this.qualifierTypes = new LogicalType[families.length][];
		String[] familyNames = hbaseSchema.getFamilyNames();
		for (int f = 0; f < families.length; f++) {
			this.qualifiers[f] = hbaseSchema.getQualifierKeys(familyNames[f]);
			DataType[] dataTypes = hbaseSchema.getQualifierDataTypes(familyNames[f]);
			this.qualifierTypes[f] = Arrays.stream(dataTypes)
				.map(DataType::getLogicalType)
				.toArray(LogicalType[]::new);
			this.reusedFamilyRows[f] = new GenericRowData(dataTypes.length);
		}
	}

	/**
	 * Returns an instance of Put that writes record to HBase table.
	 *
	 * @return The appropriate instance of Put for this use case.
	 */
	public Put createPutMutation(RowData row) {
		assert rowkeyIndex != -1;
		byte[] rowkey = serializeField(row, rowkeyIndex, rowkeyType);
		// upsert
		Put put = new Put(rowkey);
		for (int i = 0; i < fieldLength; i++) {
			if (i != rowkeyIndex) {
				int f = i > rowkeyIndex ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				RowData familyRow = row.getRow(i, qualifiers[f].length);
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					// get quantifier type idx
					LogicalType qualifierType = qualifierTypes[f][q];
					// read value
					byte[] value = serializeField(familyRow, q, qualifierType);
					put.addColumn(familyKey, qualifier, value);
				}
			}
		}
		return put;
	}

	/**
	 * Returns an instance of Delete that remove record from HBase table.
	 *
	 * @return The appropriate instance of Delete for this use case.
	 */
	public Delete createDeleteMutation(RowData row) {
		byte[] rowkey = serializeField(row, rowkeyIndex, rowkeyType);
		// delete
		Delete delete = new Delete(rowkey);
		for (int i = 0; i < fieldLength; i++) {
			if (i != rowkeyIndex) {
				int f = i > rowkeyIndex ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					delete.addColumn(familyKey, qualifier);
				}
			}
		}
		return delete;
	}

	/**
	 * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
	 *
	 * @return The appropriate instance of Scan for this use case.
	 */
	public Scan createScan() {
		Scan scan = new Scan();
		for (int f = 0; f < families.length; f++) {
			byte[] family = families[f];
			for (int q = 0; q < qualifiers[f].length; q++) {
				byte[] quantifier = qualifiers[f][q];
				scan.addColumn(family, quantifier);
			}
		}
		return scan;
	}

	/**
	 * Converts HBase {@link Result} into {@link RowData}.
	 */
	public RowData convertToRow(Result result) {
		Object rowkey = deserializeField(result.getRow(), rowkeyType);
		for (int i = 0; i < fieldLength; i++) {
			if (rowkeyIndex == i) {
				reusedRow.setField(rowkeyIndex, rowkey);
			} else {
				int f = (rowkeyIndex != -1 && i > rowkeyIndex) ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				GenericRowData familyRow = reusedFamilyRows[f];
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					// get quantifier type idx
					LogicalType qualifierType = qualifierTypes[f][q];
					// read value
					byte[] value = result.getValue(familyKey, qualifier);
					familyRow.setField(q, deserializeField(value, qualifierType));
				}
				reusedRow.setField(i, familyRow);
			}
		}
		return reusedRow;
	}

	// ------------------------------------------------------------------------------------

	private static final byte[] EMPTY_BYTES = new byte[]{};

	private static byte[] serializeField(RowData row, int ordinal, LogicalType type) {
		if (row.isNullAt(ordinal)) {
			return EMPTY_BYTES;
		}
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return Bytes.toBytes(row.getBoolean(ordinal));
			case TINYINT:
				return new byte[]{row.getByte(ordinal)};
			case SMALLINT:
				return Bytes.toBytes(row.getShort(ordinal));
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
			case DATE:
				return Bytes.toBytes(row.getInt(ordinal));
			case BIGINT:
			case INTERVAL_DAY_TIME:
			case TIME_WITHOUT_TIME_ZONE:
				return Bytes.toBytes(row.getLong(ordinal));
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_TIME_ZONE:
				// TODO: support higher precision
				long milliseconds = row.getTimestamp(ordinal, 3).getMillisecond();
				return Bytes.toBytes(milliseconds);
			case FLOAT:
				return Bytes.toBytes(row.getFloat(ordinal));
			case DOUBLE:
				return Bytes.toBytes(row.getDouble(ordinal));
			case CHAR:
			case VARCHAR:
				// get the underlying UTF-8 bytes
				return row.getString(ordinal).getBytes();
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				BigDecimal decimal = row
					.getDecimal(ordinal, decimalType.getPrecision(), decimalType.getScale())
					.toBigDecimal();
				return Bytes.toBytes(decimal);
			case BINARY:
			case VARBINARY:
				return row.getBinary(ordinal);
			default:
				throw new UnsupportedOperationException("HBase doesn't support to serialize type: " + type);
		}
	}

	private static Object deserializeField(byte[] value, LogicalType type) {
		if (value == null || value.length == 0) {
			return null;
		}
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return Bytes.toBoolean(value);
			case TINYINT:
				return value[0];
			case SMALLINT:
				return Bytes.toShort(value);
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
			case DATE:
				return Bytes.toInt(value);
			case BIGINT:
			case INTERVAL_DAY_TIME:
			case TIME_WITHOUT_TIME_ZONE:
				return Bytes.toLong(value);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_TIME_ZONE:
				// TODO: support higher precision
				long milliseconds = Bytes.toLong(value);
				return TimestampData.fromEpochMillis(milliseconds);
			case FLOAT:
				return Bytes.toFloat(value);
			case DOUBLE:
				return Bytes.toDouble(value);
			case CHAR:
			case VARCHAR:
				// reuse bytes
				return StringData.fromBytes(value);
			case DECIMAL:
				BigDecimal decimal = Bytes.toBigDecimal(value);
				DecimalType decimalType = (DecimalType) type;
				return DecimalData.fromBigDecimal(decimal, decimalType.getPrecision(), decimalType.getScale());
			case BINARY:
			case VARBINARY:
				return value;
			default:
				throw new UnsupportedOperationException("HBase doesn't support to serialize type: " + type);
		}
	}
}
