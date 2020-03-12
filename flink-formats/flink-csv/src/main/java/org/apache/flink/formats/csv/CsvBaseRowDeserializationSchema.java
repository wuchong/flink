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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.connectors.ChangelogMode;
import org.apache.flink.table.dataformats.SqlRow;
import org.apache.flink.table.dataformats.SqlDecimal;
import org.apache.flink.table.dataformats.GenericRow;
import org.apache.flink.table.dataformats.SqlString;
import org.apache.flink.table.dataformats.SqlTimestamp;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.TemporalQueries;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

/**
 * Deserialization schema from CSV to Flink Table/SQL internal data formats.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 */
@Internal
public final class CsvBaseRowDeserializationSchema implements ChangelogDeserializationSchema {
	private static final long serialVersionUID = 1L;
	public static final String DEFAULT_LINE_DELIMITER = "\n";
	public static final String DEFAULT_FIELD_DELIMITER = ",";

	private final byte[] delimiter;

	/** logical type describing the result type. **/
	private final RowType rowType;

	private final int fieldCount;

	private final FieldParser[] fieldParsers;

	public CsvBaseRowDeserializationSchema(String fieldDelimiter, RowType rowType) {
		this.delimiter = fieldDelimiter.getBytes();
		this.rowType = rowType;
		this.fieldCount = rowType.getFieldCount();
		this.fieldParsers = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(this::createParser)
			.toArray(FieldParser[]::new);
	}

	@Override
	public SqlRow deserialize(byte[] message) throws IOException {
		int startPos = 0;
		GenericRow row = new GenericRow(fieldCount);
		BytesInputView bytesView = new BytesInputView(message);
		for (int index = 0; index < fieldCount; index++) {
			// parse field
			FieldParser parser = fieldParsers[index];
			row.setField(index, parser.parseField(bytesView));
		}
		return row;
	}

	@Override
	public boolean isEndOfStream(SqlRow nextElement) {
		return false;
	}

	@Override
	public TypeInformation<SqlRow> getProducedType() {
		return new BaseRowTypeInfo(rowType);
	}

	@Override
	public ChangelogMode producedChangelogMode() {
//		ChangelogMode.newBuilder().addSupportedKind(ChangelogKind.INSERT).build();
		return ChangelogMode.insertOnly();
	}

	// -------------------------------------------------------------------------------------
	// Field Parser
	// -------------------------------------------------------------------------------------

	interface FieldParser extends Serializable {
		/**
		 * Parses the value of a field from the byte array. The start position within the byte
		 * array and the array's valid length is given.
		 * The content of the value is delimited by a field delimiter.
		 *
		 * @param bytesView The bytes view that hold the value and current position.
		 * @return The parsed field.
		 */
		Object parseField(BytesInputView bytesView);
	}

	private static final class BytesInputView {
		final byte[] bytes;
		int currentPos = 0;

		private BytesInputView(byte[] bytes) {
			this.bytes = bytes;
		}
	}

	// only support atomic type for now
	private FieldParser createParser(LogicalType type) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				// reuse byte[] without memory copying
				return this::parseToInternalString;
			case NULL:
				return bytesView -> null;
			case BOOLEAN:
				return bytesView -> Boolean.parseBoolean(readNextString(bytesView, delimiter));
			case TINYINT:
				return bytesView -> Byte.parseByte(readNextString(bytesView, delimiter));
			case SMALLINT:
				return bytesView -> Short.parseShort(readNextString(bytesView, delimiter));
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return bytesView -> Integer.parseInt(readNextString(bytesView, delimiter));
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return bytesView -> Long.parseLong(readNextString(bytesView, delimiter));
			case DATE:
				return this::convertToInternalDate;
			case TIME_WITHOUT_TIME_ZONE:
				return this::convertToInternalTime;
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return this::convertToInternalTimestamp;
			case FLOAT:
				return bytesView -> Float.parseFloat(readNextString(bytesView, delimiter));
			case DOUBLE:
				return bytesView -> Double.parseDouble(readNextString(bytesView, delimiter));
			case DECIMAL:
				return jsonNode -> convertToInternalDecimal(jsonNode, (DecimalType) type);
			case BINARY:
			case VARBINARY:
			case ROW:
			case ARRAY:
			case MAP:
			case MULTISET:
			case RAW:
			default:
				throw new UnsupportedOperationException("Not support to parse type: " + type);
		}
	}

	private int convertToInternalDate(BytesInputView bytesView) {
		String text = readNextString(bytesView, delimiter);
		LocalDate date = ISO_LOCAL_DATE.parse(text).query(TemporalQueries.localDate());
		return (int) date.toEpochDay();
	}

	private int convertToInternalTime(BytesInputView bytesView) {
		String text = readNextString(bytesView, delimiter);
		return Time.valueOf(text).toLocalTime().toSecondOfDay();
	}

	private SqlTimestamp convertToInternalTimestamp(BytesInputView bytesView) {
		String text = readNextString(bytesView, delimiter);
		return SqlTimestamp.fromTimestamp(Timestamp.valueOf(text));
	}

	private SqlDecimal convertToInternalDecimal(BytesInputView bytesView, DecimalType decimalType) {
		String text = readNextString(bytesView, delimiter);
		BigDecimal bigDecimal = new BigDecimal(text);
		return SqlDecimal.fromBigDecimal(bigDecimal, decimalType.getPrecision(), decimalType.getScale());
	}

	private SqlString parseToInternalString(BytesInputView bytesView) {
		int offset = bytesView.currentPos;
		byte[] bytes = bytesView.bytes;
		int limit = bytes.length;
		int endPos = nextStringEndPos(bytes, offset, limit, delimiter);
		int length = endPos - offset;
		// update current offset
		bytesView.currentPos = (endPos == limit) ? limit : endPos + delimiter.length;
		// creates SqlString without memory copying
		return SqlString.fromBytes(bytes, offset, length);
	}

	private static String readNextString(BytesInputView bytesView, byte[] delimiter) {
		int offset = bytesView.currentPos;
		int limit = bytesView.bytes.length;
		int endPos = nextStringEndPos(bytesView.bytes, offset, limit, delimiter);
		int length = endPos - offset;
		// update current offset
		bytesView.currentPos = (endPos == limit) ? limit : endPos + delimiter.length;;
		return new String(bytesView.bytes, offset, length, StandardCharsets.UTF_8).trim();
	}

	/**
	 * Returns the end position of a string. Sets the error state if the column is empty.
	 *
	 * @return the end position of the string or -1 if an error occurred
	 */
	private static int nextStringEndPos(byte[] bytes, int startPos, int limit, byte[] delimiter) {
		int endPos = startPos;

		final int delimLimit = limit - delimiter.length + 1;

		while (endPos < limit) {
			if (endPos < delimLimit && delimiterNext(bytes, endPos, delimiter)) {
				break;
			}
			endPos++;
		}

		if (endPos == startPos) {
			return -1;
		}
		return endPos;
	}

	/**
	 * Checks if the delimiter starts at the given start position of the byte array.
	 *
	 * Attention: This method assumes that enough characters follow the start position for the delimiter check!
	 *
	 * @param bytes The byte array that holds the value.
	 * @param startPos The index of the byte array where the check for the delimiter starts.
	 * @param delim The delimiter to check for.
	 *
	 * @return true if a delimiter starts at the given start position, false otherwise.
	 */
	public static boolean delimiterNext(byte[] bytes, int startPos, byte[] delim) {
		for(int pos = 0; pos < delim.length; pos++) {
			// check each position
			if(delim[pos] != bytes[startPos+pos]) {
				return false;
			}
		}
		return true;
	}
}
