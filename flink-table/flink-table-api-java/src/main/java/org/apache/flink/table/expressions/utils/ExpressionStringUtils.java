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

package org.apache.flink.table.expressions.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.LookupCallExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utilities to convert {@link Expression} into a string representation and back.
 *
 * <p>NOTE: we only support to convert {@link ResolvedExpression} into a string representation,
 * the parsed Expression from string maybe unresolved, e.g. {@link org.apache.flink.table.expressions.UnresolvedReferenceExpression} is converted from field reference string.
 */
@Internal
public class ExpressionStringUtils {

	private static final String CALL_FORMAT = "CALL(%s)";
	private static final String TYPE_FORMAT = "TYPE(%s)";
	private static final String VALUE_FORMAT = "VALUE(%s, %s)";
	private static final String FIELD_FORMAT = "FIELD(%s)";
	private static final String LOCAL_REF_FORMAT = "LOCAL_REF(%s)";

	/**
	 * Converts the given {@link Expression} into a string representation.
	 * The string can be used for persisting in a catalog.
	 *
	 * <p>NOTE: only {@link ResolvedExpression} can be serialized into string representation.</p>
	 * @param expr the expression to be serialized.
	 */
	public static String writeExpression(Expression expr) {
		return expr.accept(ExpressionStringify.INSTANCE);
	}

	/**
	 * Parses an expression string. Parsed expressions may not be resolved (e.g. {@link LookupCallExpression}).
	 *
	 * @param exprString a string like "CALL(PLUS, FIELD(f0), VALUE(INT, 100))"
	 * @throws ValidationException in case of parsing errors.
	 */
	public static Expression readExpression(String exprString) {
		final List<Token> tokens = tokenize(exprString);
		final TokenParser converter = new TokenParser(exprString, tokens);
		return converter.parseTokens();
	}

	// --------------------------------------------------------------------------------------------
	// Serialization
	// --------------------------------------------------------------------------------------------

	private static final class ExpressionStringify implements ExpressionVisitor<String> {

		private static final ExpressionStringify INSTANCE = new ExpressionStringify();

		/**
		 * CALL(functionName, argExpr1, argExpr2, ...).
		 */
		@Override
		public String visit(CallExpression call) {
			String functionName;
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (functionDefinition instanceof BuiltInFunctionDefinition) {
				functionName = escapeIdentifier(((BuiltInFunctionDefinition) functionDefinition).getName());
			} else {
				functionName = call.getObjectIdentifier()
					.map(ObjectIdentifier::asSerializableString)
					.orElse(escapeIdentifier(call.getFunctionDefinition().toString()));
			}

			List<String> args = call.getChildren().stream()
				.map(e -> e.accept(this))
				.collect(Collectors.toList());
			args.add(0, functionName);

			return String.format(CALL_FORMAT, String.join(", ", args));
		}

		/**
		 * VALUE('DOUBLE', 13.44).
		 */
		@Override
		public String visit(ValueLiteralExpression valueLiteral) {
			Object value = valueLiteral.getValueAs(Object.class).orElse(null);
			LogicalType type = valueLiteral.getOutputDataType().getLogicalType();
			return String.format(
				VALUE_FORMAT,
				escapeString(type.asSerializableString()),
				ValueStringifyUtils.stringifyValue(value, type));
		}

		/**
		 * FIELD(f0).
		 */
		@Override
		public String visit(FieldReferenceExpression fieldReference) {
			return String.format(
				FIELD_FORMAT,
				escapeIdentifier(fieldReference.getName()));
		}

		/**
		 * TYPE('TIMESTAMP WITHOUT TIME ZONE').
		 *
		 * <p>Only the {@link LogicalType} in {@link org.apache.flink.table.types.DataType} is serialized,
		 * because the conversion class is redundant in Expression. The conversion class is
		 * only used when entering or leaving the table ecosystem (UDF, Source, Sink).
		 */
		@Override
		public String visit(TypeLiteralExpression typeLiteral) {
			String typeString = typeLiteral.getOutputDataType().getLogicalType().asSerializableString();
			return String.format(TYPE_FORMAT, escapeString(typeString));
		}

		@Override
		public String visit(Expression other) {
			if (other instanceof LocalReferenceExpression) {
				String name = ((LocalReferenceExpression) other).getName();
				return String.format(LOCAL_REF_FORMAT, escapeIdentifier(name));
			} else {
				throw new UnsupportedOperationException(
					other.getClass().getSimpleName() + " is not string serializable for now.");
			}
		}
	}

	/**
	 * Escape identifier only if it conflict with the reserved keywords.
	 */
	private static String escapeIdentifier(String identifier) {
		if (KEYWORDS.contains(identifier.toUpperCase())) {
			return EncodingUtils.escapeIdentifier(identifier);
		} else {
			return identifier;
		}
	}

	/**
	 * Escape string literal with single quote.
	 */
	protected static String escapeString(String text) {
		return "'" + EncodingUtils.escapeSingleQuotes(text) + "'";
	}


	// --------------------------------------------------------------------------------------------
	// Deserialization
	// --------------------------------------------------------------------------------------------

	// --------------------------------------------------------------------------------------------
	// Tokenizer
	// --------------------------------------------------------------------------------------------

	private static final char CHAR_BEGIN_BRACKET = '[';
	private static final char CHAR_END_BRACKET = ']';
	private static final char CHAR_BEGIN_PARENTHESIS = '(';
	private static final char CHAR_END_PARENTHESIS = ')';
	private static final char CHAR_LIST_SEPARATOR = ',';
	private static final char CHAR_STRING = '\'';
	private static final char CHAR_IDENTIFIER = '`';
	private static final char CHAR_DOT = '.';

	private static boolean isDelimiter(char character) {
		return Character.isWhitespace(character) ||
			character == CHAR_BEGIN_BRACKET ||
			character == CHAR_END_BRACKET ||
			character == CHAR_BEGIN_PARENTHESIS ||
			character == CHAR_END_PARENTHESIS ||
			character == CHAR_LIST_SEPARATOR ||
			character == CHAR_DOT;
	}

	private static boolean isDigit(char c) {
		return c >= '0' && c <= '9';
	}

	private static List<Token> tokenize(String chars) {
		final List<Token> tokens = new ArrayList<>();
		final StringBuilder builder = new StringBuilder();
		for (int cursor = 0; cursor < chars.length(); cursor++) {
			char curChar = chars.charAt(cursor);
			switch (curChar) {
				case CHAR_BEGIN_BRACKET:
					tokens.add(new Token(TokenType.BEGIN_BRACKET, cursor, Character.toString(CHAR_BEGIN_BRACKET)));
					break;
				case CHAR_END_BRACKET:
					tokens.add(new Token(TokenType.END_BRACKET, cursor, Character.toString(CHAR_END_BRACKET)));
					break;
				case CHAR_BEGIN_PARENTHESIS:
					tokens.add(new Token(TokenType.BEGIN_PARENTHESIS, cursor, Character.toString(CHAR_BEGIN_PARENTHESIS)));
					break;
				case CHAR_END_PARENTHESIS:
					tokens.add(new Token(TokenType.END_PARENTHESIS, cursor, Character.toString(CHAR_END_PARENTHESIS)));
					break;
				case CHAR_LIST_SEPARATOR:
					tokens.add(new Token(TokenType.LIST_SEPARATOR, cursor, Character.toString(CHAR_LIST_SEPARATOR)));
					break;
				case CHAR_STRING:
					builder.setLength(0);
					cursor = consumeEscaped(builder, chars, cursor, CHAR_STRING);
					tokens.add(new Token(TokenType.LITERAL_STRING, cursor, builder.toString()));
					break;
				case CHAR_IDENTIFIER:
					builder.setLength(0);
					cursor = consumeEscaped(builder, chars, cursor, CHAR_IDENTIFIER);
					tokens.add(new Token(TokenType.IDENTIFIER, cursor, builder.toString()));
					break;
				case CHAR_DOT:
					tokens.add(new Token(TokenType.DOT_SEPARATOR, cursor, Character.toString(CHAR_DOT)));
					break;
				default:
					if (Character.isWhitespace(curChar)) {
						continue;
					}
					if (isDigit(curChar)) {
						builder.setLength(0);
						cursor = consumeInt(builder, chars, cursor);
						tokens.add(new Token(TokenType.LITERAL_INT, cursor, builder.toString()));
						break;
					}
					// identifier or keyword
					builder.setLength(0);
					cursor = consumeIdentifier(builder, chars, cursor);
					final String token = builder.toString();
					final String normalizedToken = token.toUpperCase();
					if (KEYWORDS.contains(normalizedToken)) {
						tokens.add(new Token(TokenType.KEYWORD, cursor, normalizedToken));
					} else {
						tokens.add(new Token(TokenType.IDENTIFIER, cursor, token));
					}
			}
		}
		return tokens;
	}

	private static int consumeEscaped(StringBuilder builder, String chars, int cursor, char delimiter) {
		// skip delimiter
		cursor++;
		for (; chars.length() > cursor; cursor++) {
			final char curChar = chars.charAt(cursor);
			if (curChar == delimiter && cursor + 1 < chars.length() && chars.charAt(cursor + 1) == delimiter) {
				// escaping of the escaping char e.g. "'Hello '' World'"
				cursor++;
				builder.append(curChar);
			} else if (curChar == delimiter) {
				break;
			} else {
				builder.append(curChar);
			}
		}
		return cursor;
	}

	private static int consumeInt(StringBuilder builder, String chars, int cursor) {
		for (; chars.length() > cursor && isDigit(chars.charAt(cursor)); cursor++) {
			builder.append(chars.charAt(cursor));
		}
		return cursor - 1;
	}

	private static int consumeIdentifier(StringBuilder builder, String chars, int cursor) {
		for (; cursor < chars.length() && !isDelimiter(chars.charAt(cursor)); cursor++) {
			builder.append(chars.charAt(cursor));
		}
		return cursor - 1;
	}

	private enum TokenType {

		// e.g. "CALL("
		BEGIN_PARENTHESIS,

		// e.g. "CALL(...)"
		END_PARENTHESIS,

		// e.g. "MAP["
		BEGIN_BRACKET,

		// e.g. "MAP[..]"
		END_BRACKET,

		// e.g. "CALL(a, b, c)"
		LIST_SEPARATOR,

		// identifier separator or decimal separator, e.g. "db.fun." or "VALUE(12."
		DOT_SEPARATOR,

		// e.g. "CALL" or "FIELD"
		KEYWORD,

		// e.g. "db.fun"
		IDENTIFIER,

		// e.g. "'hello'"
		LITERAL_STRING,

		// e.g. "VALUE(12"
		LITERAL_INT

	}

	private enum Keyword {
		CALL,
		TYPE,
		VALUE,
		FIELD,
		LOCAL_REF,

		// value keywords
		MAP,
		MULTISET,
		ARRAY,
		ROW,
		TRUE,
		FALSE,
		NULL
	}

	private static final Set<String> KEYWORDS = Stream.of(Keyword.values())
		.map(k -> k.toString().toUpperCase())
		.collect(Collectors.toSet());

	private static class Token {
		public final TokenType type;
		public final int cursorPosition;
		public final String value;

		public Token(TokenType type, int cursorPosition, String value) {
			this.type = type;
			this.cursorPosition = cursorPosition;
			this.value = value;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Token Parsing
	// --------------------------------------------------------------------------------------------

	private static class TokenParser {

		private final String inputString;

		private final List<Token> tokens;

		private int lastValidToken;

		private int currentToken;

		public TokenParser(String inputString, List<Token> tokens) {
			this.inputString = inputString;
			this.tokens = tokens;
			this.lastValidToken = -1;
			this.currentToken = -1;
		}

		/**
		 * Entry-point.
		 */
		private Expression parseTokens() {
			final Expression expr = parseExpression();
			if (hasRemainingTokens()) {
				nextToken();
				throw parsingError("Unexpected token: " + token().value);
			}
			return expr;
		}

		private int lastCursor() {
			if (lastValidToken < 0) {
				return 0;
			}
			return tokens.get(lastValidToken).cursorPosition + 1;
		}

		private ValidationException parsingError(String cause, @Nullable Throwable t) {
			return new ValidationException(
				String.format(
					"Could not parse type at position %d: %s\n Input type string: %s",
					lastCursor(),
					cause,
					inputString),
				t);
		}

		private ValidationException parsingError(String cause) {
			return parsingError(cause, null);
		}

		private boolean hasRemainingTokens() {
			return currentToken + 1 < tokens.size();
		}

		private Token token() {
			return tokens.get(currentToken);
		}

		private Keyword tokenAsKeyword() {
			return Keyword.valueOf(token().value);
		}

		private String tokenAsString() {
			return token().value;
		}

		private void nextToken() {
			this.currentToken++;
			if (currentToken >= tokens.size()) {
				throw parsingError("Unexpected end.");
			}
			lastValidToken = this.currentToken - 1;
		}

		private void nextToken(TokenType type) {
			nextToken();
			final Token token = token();
			if (token.type != type) {
				throw parsingError("<" + type.name() + "> expected but was <" + token.type + ">.");
			}
		}

		private void nextToken(Keyword keyword) {
			nextToken(TokenType.KEYWORD);
			final Token token = token();
			if (!keyword.equals(Keyword.valueOf(token.value))) {
				throw parsingError("Keyword '" + keyword + "' expected but was '" + token.value + "'.");
			}
		}

		private boolean hasNextToken(TokenType... types) {
			if (currentToken + types.length + 1 > tokens.size()) {
				return false;
			}
			for (int i = 0; i < types.length; i++) {
				final Token lookAhead = tokens.get(currentToken + i + 1);
				if (lookAhead.type != types[i]) {
					return false;
				}
			}
			return true;
		}

		private boolean hasNextToken(Keyword... keywords) {
			if (currentToken + keywords.length + 1 > tokens.size()) {
				return false;
			}
			for (int i = 0; i < keywords.length; i++) {
				final Token lookAhead = tokens.get(currentToken + i + 1);
				if (lookAhead.type != TokenType.KEYWORD ||
					keywords[i] != Keyword.valueOf(lookAhead.value)) {
					return false;
				}
			}
			return true;
		}

		private Expression parseExpression() {
			nextToken(TokenType.KEYWORD);
			switch (tokenAsKeyword()) {
				case CALL:
					return parseCallExpression();
				case TYPE:
					return parseTypeExpression();
				case VALUE:
					return parseValueExpression();
				case FIELD:
				case LOCAL_REF:
					return parseReferenceExpression();
				default:
					throw parsingError("Unsupported expression: " + token().value);
			}
		}

		private Expression parseCallExpression() {
			nextToken(TokenType.BEGIN_PARENTHESIS);
			String functionName = parseIdentifier();
			List<Expression> args = parseCallArguments();
			nextToken(TokenType.END_PARENTHESIS);
			return new LookupCallExpression(functionName, args);
		}

		private String parseIdentifier() {
			nextToken(TokenType.IDENTIFIER);
			List<String> parts = new ArrayList<>();
			parts.add(tokenAsString());
			if (hasNextToken(TokenType.DOT_SEPARATOR)) {
				nextToken(TokenType.DOT_SEPARATOR);
				nextToken(TokenType.IDENTIFIER);
				parts.add(tokenAsString());
			}
			if (hasNextToken(TokenType.DOT_SEPARATOR)) {
				nextToken(TokenType.DOT_SEPARATOR);
				nextToken(TokenType.IDENTIFIER);
				parts.add(tokenAsString());
			}
			return String.join(".", parts);
		}

		private List<Expression> parseCallArguments() {
			List<Expression> arguments = new ArrayList<>();
			while (!hasNextToken(TokenType.END_PARENTHESIS)) {
				nextToken(TokenType.LIST_SEPARATOR);
				arguments.add(parseExpression());
			}
			return arguments;
		}

		private Expression parseTypeExpression() {
			nextToken(TokenType.BEGIN_PARENTHESIS);
			nextToken(TokenType.LITERAL_STRING);
			String typeString = tokenAsString();
			LogicalType logicalType = LogicalTypeParser.parse(typeString);
			nextToken(TokenType.END_PARENTHESIS);
			return new TypeLiteralExpression(TypeConversions.fromLogicalToDataType(logicalType));
		}

		private Expression parseReferenceExpression() {
			nextToken(TokenType.BEGIN_PARENTHESIS);
			String field = parseIdentifier();
			nextToken(TokenType.END_PARENTHESIS);
			return new UnresolvedReferenceExpression(field);
		}

		private Expression parseValueExpression() {
			nextToken(TokenType.BEGIN_PARENTHESIS);
			nextToken(TokenType.LITERAL_STRING);
			String typeString = tokenAsString();
			LogicalType logicalType = LogicalTypeParser.parse(typeString);
			nextToken(TokenType.LIST_SEPARATOR);
			Object value = parseValueByType(logicalType);
			nextToken(TokenType.END_PARENTHESIS);
			return new ValueLiteralExpression(
				value,
				convertToDataTypeWithAdaptedConversionClass(logicalType, value));
		}

		/**
		 * Converts LogicalType to DataType with adapted conversion class (non-default conversion class).
		 */
		private DataType convertToDataTypeWithAdaptedConversionClass(LogicalType logicalType, Object obj) {
			DataType dataType = TypeConversions.fromLogicalToDataType(logicalType);
			switch (logicalType.getTypeRoot()) {
				case TIMESTAMP_WITH_TIME_ZONE:
					// the instance might be OffsetDateTime or ZonedDateTime
					return obj != null ? dataType.bridgedTo(obj.getClass()) : dataType;
				case INTERVAL_YEAR_MONTH:
					// default class of YearMonthIntervalType is Period, but we use Integer instead.
					return dataType.bridgedTo(Integer.class);
				case INTERVAL_DAY_TIME:
					// default class of DayTimeIntervalType, but we use Long instead
					return dataType.bridgedTo(Long.class);
				default:
					return dataType;
			}
		}

		/**
		 * @see ValueStringifyUtils#stringifyValue(Object, LogicalType)
		 */
		private Object parseValueByType(LogicalType type) {
			if (hasNextToken(Keyword.NULL)) {
				nextToken(Keyword.NULL);
				return null;
			}
			switch (type.getTypeRoot()) {
				case NULL:
					return null;
				case BOOLEAN:
					nextToken(TokenType.KEYWORD);
					return Boolean.valueOf(tokenAsString());
				case TINYINT:
					return parseBigDecimalValue().byteValueExact();
				case SMALLINT:
					return parseBigDecimalValue().shortValueExact();
				case INTEGER:
				case INTERVAL_YEAR_MONTH:
					return parseBigDecimalValue().intValueExact();
				case BIGINT:
				case INTERVAL_DAY_TIME:
					return parseBigDecimalValue().longValueExact();
				case FLOAT:
					return parseBigDecimalValue().floatValue();
				case DOUBLE:
					return parseBigDecimalValue().doubleValue();
				case DECIMAL:
					return parseBigDecimalValue();
				case DATE:
					return LocalDate.parse(parseStringValue());
				case TIME_WITHOUT_TIME_ZONE:
					return LocalTime.parse(parseStringValue());
				case TIMESTAMP_WITHOUT_TIME_ZONE:
					return LocalDateTime.parse(parseStringValue());
				case TIMESTAMP_WITH_TIME_ZONE:
					return parseTimestampWithTimeZoneValue();
				case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
					return parseInstantValue();
				case CHAR:
				case VARCHAR:
					return parseStringValue();
				case BINARY:
				case VARBINARY:
					return EncodingUtils.decodeBase64ToBytes(parseStringValue());
				case ANY:
					return parseGenericType((AnyType<?>) type);
				case SYMBOL:
					return parseEnumValue((SymbolType) type);
				case MAP:
					return parseMapValue((MapType) type);
				case MULTISET:
					return parseMultisetValue((MultisetType) type);
				case ARRAY:
					return parseArrayValue((ArrayType) type);
				case ROW:
					return parseRowValue((RowType) type);
				default:
					throw parsingError("Unsupported type: " + token().value);
			}
		}

		private BigDecimal parseBigDecimalValue() {
			nextToken(TokenType.LITERAL_INT);
			String value;
			String intVal = tokenAsString();
			// has fractional part
			if (hasNextToken(TokenType.DOT_SEPARATOR)) {
				nextToken(TokenType.DOT_SEPARATOR);
				nextToken(TokenType.LITERAL_INT);
				String fraction = tokenAsString();
				value = intVal + "." + fraction;
			} else {
				value = intVal;
			}
			return new BigDecimal(value);
		}

		private String parseStringValue() {
			nextToken(TokenType.LITERAL_STRING);
			return tokenAsString();
		}

		private Object parseTimestampWithTimeZoneValue() {
			// either OffsetDateTime or ZonedDateTime
			// first try parse to OffsetDateTime, then try ZonedDateTime, otherwise throw exception
			String text = parseStringValue();
			try {
				return OffsetDateTime.parse(text);
			} catch (DateTimeParseException e) {
				// fallback to ZonedDateTime
				return ZonedDateTime.parse(text);
			}
		}

		private Instant parseInstantValue() {
			String text = parseStringValue();
			String[] parts = text.split("\\.");
			if (parts.length != 2) {
				throw new IllegalStateException(
					"The serialized string is corrupt, can't be parsed to Instant, " +
						"the format should be in '1570796390.988000000' format.");
			}
			long seconds = Long.valueOf(parts[0]);
			long nanos = Long.valueOf(parts[1]);
			return Instant.ofEpochSecond(seconds, nanos);
		}

		@SuppressWarnings("unchecked")
		private Object parseEnumValue(SymbolType symbolType) {
			nextToken(TokenType.IDENTIFIER);
			String text = tokenAsString();
			return Enum.valueOf(symbolType.getDefaultConversion(), text);
		}

		private Map<Object, Object> parseMapValue(MapType mapType) {
			LogicalType keyType = mapType.getKeyType();
			LogicalType valueType = mapType.getValueType();
			Map<Object, Object> map = new HashMap<>();
			nextToken(Keyword.MAP);
			nextToken(TokenType.BEGIN_BRACKET);
			boolean isFirst = true;
			while (!hasNextToken(TokenType.END_BRACKET)) {
				if (isFirst) {
					isFirst = false;
				} else {
					nextToken(TokenType.LIST_SEPARATOR);
				}
				Object key = parseValueByType(keyType);
				nextToken(TokenType.LIST_SEPARATOR);
				Object value = parseValueByType(valueType);
				map.put(key, value);
			}
			nextToken(TokenType.END_BRACKET);
			return map;
		}

		private Map<Object, Integer> parseMultisetValue(MultisetType multisetType) {
			LogicalType elementType = multisetType.getElementType();
			Map<Object, Integer> map = new HashMap<>();
			nextToken(Keyword.MULTISET);
			nextToken(TokenType.BEGIN_BRACKET);
			boolean isFirst = true;
			while (!hasNextToken(TokenType.END_BRACKET)) {
				if (isFirst) {
					isFirst = false;
				} else {
					nextToken(TokenType.LIST_SEPARATOR);
				}
				Object element = parseValueByType(elementType);
				nextToken(TokenType.LIST_SEPARATOR);
				int num = parseBigDecimalValue().intValueExact();
				map.put(element, num);
			}
			nextToken(TokenType.END_BRACKET);
			return map;
		}

		private Object parseArrayValue(ArrayType arrayType) {
			LogicalType elementType = arrayType.getElementType();
			List<Object> list = new ArrayList<>();
			nextToken(Keyword.ARRAY);
			nextToken(TokenType.BEGIN_BRACKET);
			boolean isFirst = true;
			Object elementInstance = null;
			while (!hasNextToken(TokenType.END_BRACKET)) {
				if (isFirst) {
					isFirst = false;
				} else {
					nextToken(TokenType.LIST_SEPARATOR);
				}
				Object element = parseValueByType(elementType);
				if (element != null) {
					elementInstance = element;
				}
				list.add(element);
			}
			// list.toArray() returns Object[] but we need the concrete array type, e.g. String[]
			Class elementClass = convertToDataTypeWithAdaptedConversionClass(elementType, elementInstance)
				.getConversionClass();
			Object array = Array.newInstance(elementClass, list.size());
			for (int i = 0; i < list.size(); i++) {
				Array.set(array, i, list.get(i));
			}
			nextToken(TokenType.END_BRACKET);
			return array;
		}

		private Row parseRowValue(RowType rowType) {
			Row row = new Row(rowType.getFieldCount());
			nextToken(Keyword.ROW);
			nextToken(TokenType.BEGIN_PARENTHESIS);
			for (int i = 0; i < rowType.getFieldCount(); i++) {
				if (i > 0) {
					nextToken(TokenType.LIST_SEPARATOR);
				}
				Object field = parseValueByType(rowType.getTypeAt(i));
				row.setField(i, field);
			}
			nextToken(TokenType.END_PARENTHESIS);
			return row;
		}

		private <T> T parseGenericType(AnyType<T> type) {
			byte[] bytes = EncodingUtils.decodeBase64ToBytes(parseStringValue());
			try {
				return InstantiationUtil.deserializeFromByteArray(type.getTypeSerializer(), bytes);
			} catch (IOException e) {
				throw parsingError("failed to deserialize from byte array", e);
			}
		}
	}
}
