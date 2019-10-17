package org.apache.flink.table.expressions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.expressions.utils.ExpressionStringUtils;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.planner.expressions.PlannerTypeInferenceUtilImpl;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.types.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.api.EnvironmentSettings.DEFAULT_BUILTIN_CATALOG;
import static org.apache.flink.table.api.EnvironmentSettings.DEFAULT_BUILTIN_DATABASE;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.intervalOfMillis;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.intervalOfMonths;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.lookupCall;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EQUALS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EXTRACT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.GET;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.PLUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.WINDOW_START;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ExpressionStringUtils}.
 *
 * <p>TODO: move this test class to flink-table-api-java module until we don't need to depend on {@link PlannerTypeInferenceUtilImpl}.
 */
@RunWith(Parameterized.class)
public class ExpressionStringTest {

	@Parameterized.Parameters(name = "{0}")
	public static List<TestSpec> testData() {
		Timestamp sqlTimestamp = Timestamp.valueOf("2006-11-03 00:00:00.123456789");
		LocalDateTime localDateTime = LocalDateTime.of(2006, 11, 3, 0, 0, 0, 123456789);
		Date sqlDate = Date.valueOf("2006-11-03");
		LocalDate localDate = LocalDate.of(2006, 11, 3);
		Time sqlTime = Time.valueOf("08:08:40");
		LocalTime localTime = LocalTime.of(8, 8, 40);
		LocalTime localTime9 = LocalTime.of(8, 8, 40, 123456789);
		Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
		OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.of("+01:00"));
		ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.of("Europe/Paris"));
		Map<String, Integer> map = new HashMap<>();
		map.put("key1", 1);
		map.put("key2", 2);
		map.put("key3", 3);

		return Arrays.asList(

			// -------------------------------- Values --------------------------------------

			TestSpec
				.expression(valueLiteral(true))
				.expectString("VALUE('BOOLEAN NOT NULL', true)"),

			TestSpec
				.expression(valueLiteral(200))
				.expectString("VALUE('INT NOT NULL', 200)"),

			TestSpec
				.expression(valueLiteral(2.44444444443d))
				.expectString("VALUE('DOUBLE NOT NULL', 2.44444444443)"),

			TestSpec
				.expression(valueLiteral(new BigDecimal("2.0123456789")))
				.expectString("VALUE('DECIMAL(11, 10) NOT NULL', 2.0123456789)"),

			TestSpec
				.expression(valueLiteral("Hello ' World"))
				.expectString("VALUE('CHAR(13) NOT NULL', 'Hello '' World')"),

			TestSpec
				.expression(valueLiteral("Hello ' World", DataTypes.STRING()))
				.expectString("VALUE('VARCHAR(2147483647)', 'Hello '' World')"),

			TestSpec
				.expression(valueLiteral(sqlTimestamp))
				.expectString("VALUE('TIMESTAMP(9) NOT NULL', '2006-11-03T00:00:00.123456789')")
				.expectParsedExpr(valueLiteral(localDateTime)),

			TestSpec
				.expression(valueLiteral(localDateTime))
				.expectString("VALUE('TIMESTAMP(9) NOT NULL', '2006-11-03T00:00:00.123456789')"),

			TestSpec
				.expression(valueLiteral(sqlDate))
				.expectString("VALUE('DATE NOT NULL', '2006-11-03')")
				.expectParsedExpr(valueLiteral(localDate)),

			TestSpec
				.expression(valueLiteral(localDate))
				.expectString("VALUE('DATE NOT NULL', '2006-11-03')"),

			TestSpec
				.expression(valueLiteral(sqlTime))
				.expectString("VALUE('TIME(0) NOT NULL', '08:08:40')")
				.expectParsedExpr(valueLiteral(localTime)),

			TestSpec
				.expression(valueLiteral(localTime9))
				.expectString("VALUE('TIME(9) NOT NULL', '08:08:40.123456789')"),

			TestSpec
				.expression(valueLiteral(instant))
				.expectString("VALUE('TIMESTAMP(9) WITH LOCAL TIME ZONE NOT NULL', '1162512000.123456789')"),

			TestSpec
				.expression(valueLiteral(offsetDateTime))
				.expectString("VALUE('TIMESTAMP(9) WITH TIME ZONE NOT NULL', '2006-11-03T00:00:00.123456789+01:00')"),

			TestSpec
				.expression(valueLiteral(zonedDateTime))
				.expectString("VALUE('TIMESTAMP(9) WITH TIME ZONE NOT NULL', '2006-11-03T00:00:00.123456789+01:00[Europe/Paris]')"),

			TestSpec
				.expression(intervalOfMillis(3000))
				.expectString("VALUE('INTERVAL SECOND(3) NOT NULL', 3000)"),

			TestSpec
				.expression(intervalOfMonths(2))
				.expectString("VALUE('INTERVAL MONTH NOT NULL', 2)"),

			TestSpec
				.expression(valueLiteral(TimeIntervalUnit.DAY_TO_MINUTE))
				.expectString("VALUE('SYMBOL(''org.apache.flink.table.expressions.TimeIntervalUnit'') NOT NULL', DAY_TO_MINUTE)"),

			TestSpec
				.expression(valueLiteral(TimePointUnit.MILLISECOND))
				.expectString("VALUE('SYMBOL(''org.apache.flink.table.expressions.TimePointUnit'') NOT NULL', MILLISECOND)"),

			// binary

			TestSpec
				.expression(valueLiteral("abc".getBytes(Charset.forName("UTF-8"))))
				.expectString("VALUE('BINARY(3) NOT NULL', 'YWJj')"),

			TestSpec
				.expression(valueLiteral("abc".getBytes(Charset.forName("UTF-8")), DataTypes.BYTES()))
				.expectString("VALUE('VARBINARY(2147483647)', 'YWJj')"),

			// any

			TestSpec
				.expression(valueLiteral(
					new TestSpec(),
					DataTypes.ANY(
						TestSpec.class,
						createAnyType(TestSpec.class).getTypeSerializer())))
				.expectString(String.format(
					"VALUE('%s', 'VwEAAAAA')",
					EncodingUtils.escapeSingleQuotes(
						createAnyType(TestSpec.class).asSerializableString()))),

			// map

			TestSpec
				.expression(valueLiteral(map, DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())))
				.expectString("VALUE('MAP<VARCHAR(2147483647), INT>', MAP['key1', 1, 'key2', 2, 'key3', 3])"),

			TestSpec
				.expression(valueLiteral(map, DataTypes.MULTISET(DataTypes.STRING())))
				.expectString("VALUE('MULTISET<VARCHAR(2147483647)>', MULTISET['key1', 1, 'key2', 2, 'key3', 3])"),

			// array

			TestSpec
				.expression(valueLiteral(new String[]{"a", "b", "c"}))
				.expectString("VALUE('ARRAY<CHAR(1)> NOT NULL', ARRAY['a', 'b', 'c'])"),

			// row

			TestSpec
				.expression(valueLiteral(
					Row.of("str", 12, new BigDecimal("12.123456789"), true),
					DataTypes.ROW(
						DataTypes.FIELD("f0", DataTypes.STRING()),
						DataTypes.FIELD("f1", DataTypes.INT()),
						DataTypes.FIELD("f2", DataTypes.DECIMAL(11, 9)),
						DataTypes.FIELD("f3", DataTypes.BOOLEAN()))))
				.expectString("VALUE(" +
					"'ROW<`f0` VARCHAR(2147483647), `f1` INT, `f2` DECIMAL(11, 9), `f3` BOOLEAN>', " +
					"ROW('str', 12, 12.123456789, true))"),

			// error message testing

			TestSpec
				.string("VALUE(BOOLEAN NOT NULL, true)")
				.expectErrorMessage("<LITERAL_STRING> expected"),

			TestSpec
				.string("VALUE('VARCHAR(2147483647)', 200)")
				.expectErrorMessage("<LITERAL_STRING> expected"),

			TestSpec
				.string("VALUE('TIMESTAMP(9) WITH LOCAL TIME ZONE', 1162512000.123456789)")
				.expectErrorMessage("<LITERAL_STRING> expected"),

			// -------------------------------- Types --------------------------------------

			TestSpec
				.expression(typeLiteral(DataTypes.DECIMAL(10, 3)))
				.expectString("TYPE('DECIMAL(10, 3)')"),

			TestSpec
				.expression(typeLiteral(DataTypes.STRING()))
				.expectString("TYPE('VARCHAR(2147483647)')"),

			TestSpec
				.expression(typeLiteral(DataTypes.TIMESTAMP_WITH_TIME_ZONE(3)))
				.expectString("TYPE('TIMESTAMP(3) WITH TIME ZONE')"),

			TestSpec
				.expression(typeLiteral(DataTypes.ROW(
					DataTypes.FIELD("f0", DataTypes.BIGINT()),
					DataTypes.FIELD("f1", DataTypes.ARRAY(DataTypes.TIMESTAMP(9))),
					DataTypes.FIELD("f2", DataTypes.DOUBLE()),
					DataTypes.FIELD("f3", DataTypes.BOOLEAN()))))
				.expectString("TYPE('ROW<`f0` BIGINT, `f1` ARRAY<TIMESTAMP(9)>, `f2` DOUBLE, `f3` BOOLEAN>')"),

			// --------------------------- CALL and FIELD -----------------------------

			TestSpec
				.expression(resolve(unresolvedRef("field")))
				.expectString("FIELD(`field`)"),

			TestSpec
				.expression(resolve(unresolvedCall(PLUS, unresolvedRef("f0"), valueLiteral(100))))
				.expectString("CALL(plus, FIELD(f0), VALUE('INT NOT NULL', 100))"),

			TestSpec
				.expression(resolve(unresolvedCall(CAST, unresolvedRef("f3"), typeLiteral(DataTypes.BOOLEAN()))))
				.expectString("CALL(cast, FIELD(f3), TYPE('BOOLEAN'))"),

			TestSpec
				.expression(resolve(unresolvedCall(EXTRACT, valueLiteral(TimeIntervalUnit.DAY), unresolvedRef("f5"))))
				.expectString("CALL(extract, VALUE('SYMBOL(''org.apache.flink.table.expressions.TimeIntervalUnit'') NOT NULL', DAY), FIELD(f5))"),

			// (true AND (f6 = f5.q1[1]))
			TestSpec
				.expression(resolve(unresolvedCall(
					AND,
					valueLiteral(true),
					unresolvedCall(
						EQUALS,
						unresolvedRef("f5"),
						unresolvedCall(
							AT,
							unresolvedCall(GET, unresolvedRef("f4"), valueLiteral("q1")),
							valueLiteral(1))))))
				.expectString("CALL(and, " +
					"VALUE('BOOLEAN NOT NULL', true), " +
					"CALL(equals, FIELD(f5)," +
					" CALL(at, CALL(get, FIELD(f4), VALUE('CHAR(2) NOT NULL', 'q1')), VALUE('INT NOT NULL', 1))))"),

			TestSpec
				.expression(resolve(unresolvedCall(
					new ScalarFunctionDefinition("dummy", DUMMY_FUNCTION), unresolvedRef("f0"))))
				.expectString("CALL(dummy, FIELD(f0))"),

			TestSpec
				.expression(resolve(lookupCall("dummy", valueLiteral(12))))
				.expectString("CALL(dummy, VALUE('INT NOT NULL', 12))"),

			TestSpec
				.expression(resolve(unresolvedCall(WINDOW_START, unresolvedRef("w"))))
				.expectString("CALL(start, LOCAL_REF(w))"),

			TestSpec
				.expression(unresolvedCall(PLUS, unresolvedRef("f0"), valueLiteral(100)))
				.expectErrorMessage("UnresolvedCallExpression is not string serializable for now.")
		);
	}

	@Parameterized.Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testSerialize() {
		if (testSpec.expression != null && testSpec.exprString != null) {
			assertEquals(
				testSpec.exprString,
				ExpressionStringUtils.writeExpression(testSpec.expression)
			);
		}
	}

	@Test
	public void testDeserialize() {
		Expression expected = null;
		if (testSpec.parsedExpr != null) {
			expected = testSpec.parsedExpr;
		} else if (testSpec.expression != null) {
			expected = testSpec.expression;
		}

		if (testSpec.exprString != null && expected != null) {
			assertEquals(
				expected,
				resolve(ExpressionStringUtils.readExpression(testSpec.exprString))
			);
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void testErrorMessage() {
		if (testSpec.expectedErrorMessage != null) {
			thrown.expectMessage(testSpec.expectedErrorMessage);

			if (testSpec.expression != null) {
				ExpressionStringUtils.writeExpression(testSpec.expression);
			}

			if (testSpec.exprString != null) {
				ExpressionStringUtils.readExpression(testSpec.exprString);
			}
		}
	}


	// --------------------------------------------------------------------------------------------

	private static class TestSpec {
		private @Nullable Expression expression;
		private @Nullable String exprString;
		private @Nullable Expression parsedExpr;
		private @Nullable String expectedErrorMessage;

		static TestSpec expression(Expression expr) {
			TestSpec spec = new TestSpec();
			spec.expression = expr;
			return spec;
		}

		static TestSpec string(String exprString) {
			TestSpec spec = new TestSpec();
			spec.exprString = exprString;
			return spec;
		}

		TestSpec expectString(String exprString) {
			this.exprString = exprString;
			return this;
		}

		TestSpec expectParsedExpr(Expression expectedExpr) {
			checkNotNull(exprString);
			this.parsedExpr = expectedExpr;
			return this;
		}

		TestSpec expectErrorMessage(String expectedErrorMessage) {
			this.expectedErrorMessage = expectedErrorMessage;
			return this;
		}

		@Override
		public String toString() {
			List<String> builder = new ArrayList<>();
			if (expression != null) {
				builder.add("expr: " + expression);
			}
			if (exprString != null) {
				builder.add("string: " + exprString);
			}
			if (parsedExpr != null) {
				builder.add("parsedExpr: " + parsedExpr);
			}
			if (expectedErrorMessage != null) {
				builder.add("error:" + expectedErrorMessage);
			}
			return String.join(", ", builder);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			TestSpec testSpec = (TestSpec) o;
			return Objects.equals(expression, testSpec.expression) &&
				Objects.equals(exprString, testSpec.exprString) &&
				Objects.equals(parsedExpr, testSpec.parsedExpr) &&
				Objects.equals(expectedErrorMessage, testSpec.expectedErrorMessage);
		}

		@Override
		public int hashCode() {
			return Objects.hash(expression, exprString, parsedExpr, expectedErrorMessage);
		}
	}

	private static <T> AnyType<T> createAnyType(Class<T> clazz) {
		return new AnyType<>(clazz, new KryoSerializer<>(clazz, new ExecutionConfig()));
	}

	// -------------------------------------------------------------------------------------------

	private static final FunctionCatalog functionCatalog = new FunctionCatalog(
		new CatalogManager(
			DEFAULT_BUILTIN_CATALOG,
			new GenericInMemoryCatalog(
				DEFAULT_BUILTIN_CATALOG,
				DEFAULT_BUILTIN_DATABASE)
		)
	);

	private static final TableSchema schema = TableSchema.builder()
		.field("field", DataTypes.BOOLEAN()) // field name is a keyword
		.field("f0", DataTypes.INT())
		.field("f1", DataTypes.BIGINT())
		.field("f2", DataTypes.DECIMAL(10, 0))
		.field("f3", DataTypes.STRING())
		.field("f4", DataTypes.ROW(
			DataTypes.FIELD("q0", DataTypes.BIGINT()),
			DataTypes.FIELD("q1", DataTypes.ARRAY(DataTypes.TIMESTAMP(3))),
			DataTypes.FIELD("q2", DataTypes.DOUBLE()),
			DataTypes.FIELD("q3", DataTypes.BOOLEAN())))
		.field("f5", DataTypes.TIMESTAMP(3))
		.build();

	private static ExpressionResolver resolver = ExpressionResolver.resolverFor(
		name -> Optional.empty(),
		functionCatalog,
		new CatalogQueryOperation(
			ObjectIdentifier.of(DEFAULT_BUILTIN_CATALOG, DEFAULT_BUILTIN_DATABASE, "myTable"),
			schema)
	).withLocalReferences(new LocalReferenceExpression("w", DataTypes.TIMESTAMP(3))).build();

	/**
	 * Dummy ScalarFunction used for testing.
	 */
	public static final class DummyFunction extends ScalarFunction {
		private static final long serialVersionUID = -271454675582032537L;
		public int eval(int a) {
			return a + 1;
		}
	}

	private static final ScalarFunction DUMMY_FUNCTION = new DummyFunction();

	// TODO: remove this until we have the unified TypeInferenceUtil.
	static {
		functionCatalog.setPlannerTypeInferenceUtil(PlannerTypeInferenceUtilImpl.INSTANCE);
		functionCatalog.registerTempSystemScalarFunction("dummy", DUMMY_FUNCTION);
	}

	private static ResolvedExpression resolve(Expression expr) {
		return resolver.resolve(Collections.singletonList(expr)).get(0);
	}
}
