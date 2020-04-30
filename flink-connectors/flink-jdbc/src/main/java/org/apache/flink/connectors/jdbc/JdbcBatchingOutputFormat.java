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

package org.apache.flink.connectors.jdbc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.jdbc.dialect.JdbcDialect;
import org.apache.flink.connectors.jdbc.executor.JdbcBatchStatementExecutor;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link AbstractJdbcOutputFormat}.
 */
public class JdbcBatchingOutputFormat<In, JdbcIn, JdbcExec extends JdbcBatchStatementExecutor<JdbcIn>> extends AbstractJdbcOutputFormat<In> {
	interface RecordExtractor<F, T> extends Function<F, T>, Serializable {
		static <T> RecordExtractor<T, T> identity() {
			return x -> x;
		}
	}

	interface StatementExecutorFactory<T extends JdbcBatchStatementExecutor<?>> extends Function<RuntimeContext, T>, Serializable {
	}

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JdbcBatchingOutputFormat.class);

	private final JdbcExecutionOptions executionOptions;
	private final StatementExecutorFactory<JdbcExec> statementExecutorFactory;
	private final RecordExtractor<In, JdbcIn> jdbcRecordExtractor;

	private transient JdbcExec jdbcStatementExecutor;
	private transient int batchCount = 0;
	private transient volatile boolean closed = false;

	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;

	JdbcBatchingOutputFormat(
			@Nonnull JdbcConnectionProvider connectionProvider,
			@Nonnull JdbcExecutionOptions executionOptions,
			@Nonnull StatementExecutorFactory<JdbcExec> statementExecutorFactory,
			@Nonnull RecordExtractor<In, JdbcIn> recordExtractor) {
		super(connectionProvider);
		this.executionOptions = checkNotNull(executionOptions);
		this.statementExecutorFactory = checkNotNull(statementExecutorFactory);
		this.jdbcRecordExtractor = checkNotNull(recordExtractor);
	}

	/**
	 * Connects to the target database and initializes the prepared statement.
	 *
	 * @param taskNumber The number of the parallel instance.
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
		if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
			this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("jdbc-upsert-output-format"));
			this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
				synchronized (JdbcBatchingOutputFormat.this) {
					if (!closed) {
						try {
							flush();
						} catch (Exception e) {
							flushException = e;
						}
					}
				}
			}, executionOptions.getBatchIntervalMs(), executionOptions.getBatchIntervalMs(), TimeUnit.MILLISECONDS);
		}
	}

	private JdbcExec createAndOpenStatementExecutor(StatementExecutorFactory<JdbcExec> statementExecutorFactory) throws IOException {
		JdbcExec exec = statementExecutorFactory.apply(getRuntimeContext());
		try {
			exec.open(connection);
		} catch (SQLException e) {
			throw new IOException("unable to open JDBC writer", e);
		}
		return exec;
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to JDBC failed.", flushException);
		}
	}

	@Override
	public final synchronized void writeRecord(In record) throws IOException {
		checkFlushException();

		try {
			addToBatch(record, jdbcRecordExtractor.apply(record));
			batchCount++;
			if (batchCount >= executionOptions.getBatchSize()) {
				flush();
			}
		} catch (Exception e) {
			throw new IOException("Writing records to JDBC failed.", e);
		}
	}

	void addToBatch(In original, JdbcIn extracted) throws SQLException {
		jdbcStatementExecutor.addToBatch(extracted);
	}

	@Override
	public synchronized void flush() throws IOException {
		checkFlushException();

		for (int i = 1; i <= executionOptions.getMaxRetries(); i++) {
			try {
				attemptFlush();
				batchCount = 0;
				break;
			} catch (SQLException e) {
				LOG.error("JDBC executeBatch error, retry times = {}", i, e);
				if (i >= executionOptions.getMaxRetries()) {
					throw new IOException(e);
				}
				try {
					Thread.sleep(1000 * i);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IOException("unable to flush; interrupted while doing another attempt", e);
				}
			}
		}
	}

	void attemptFlush() throws SQLException {
		jdbcStatementExecutor.executeBatch();
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 */
	@Override
	public synchronized void close() {
		if (!closed) {
			closed = true;

			checkFlushException();

			if (this.scheduledFuture != null) {
				scheduledFuture.cancel(false);
				this.scheduler.shutdown();
			}

			if (batchCount > 0) {
				try {
					flush();
				} catch (Exception e) {
					throw new RuntimeException("Writing records to JDBC failed.", e);
				}
			}

			try {
				jdbcStatementExecutor.close();
			} catch (SQLException e) {
				LOG.warn("Close JDBC writer failed.", e);
			}
		}
		super.close();
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for a {@link JdbcBatchingOutputFormat}.
	 */
	public static class Builder {
		private JdbcOptions options;
		private String[] fieldNames;
		private String[] keyFields;
		private JdbcDataType[] fieldTypes;
		private JdbcExecutionOptions.Builder executionOptionsBuilder = JdbcExecutionOptions.builder();

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JdbcOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * required, field names of this jdbc sink.
		 */
		Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		/**
		 * required, upsert unique keys.
		 */
		Builder setKeyFields(String[] keyFields) {
			this.keyFields = keyFields;
			return this;
		}

		/**
		 * required, field types of this jdbc sink.
		 */
		Builder setFieldTypes(JdbcDataType[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		/**
		 * optional, flush max size (includes all append, upsert and delete records),
		 * over this number of records, will flush data.
		 */
		Builder setFlushMaxSize(int flushMaxSize) {
			executionOptionsBuilder.withBatchSize(flushMaxSize);
			return this;
		}

		/**
		 * optional, flush interval mills, over this time, asynchronous threads will flush data.
		 */
		Builder setFlushIntervalMills(long flushIntervalMills) {
			executionOptionsBuilder.withBatchIntervalMs(flushIntervalMills);
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		Builder setMaxRetryTimes(int maxRetryTimes) {
			executionOptionsBuilder.withMaxRetries(maxRetryTimes);
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCUpsertOutputFormat
		 */
		public JdbcBatchingOutputFormat<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> build() {
			checkNotNull(options, "No options supplied.");
			checkNotNull(fieldNames, "No fieldNames supplied.");
			JdbcDmlOptions dml = JdbcDmlOptions.builder()
				.withTableName(options.getTableName())
				.withDialect(options.getDialect())
				.withFieldNames(fieldNames)
				.withKeyFields(keyFields)
				.withFieldTypes(fieldTypes)
				.build();
			if (dml.getKeyFields().isPresent() && dml.getKeyFields().get().length > 0) {
				return new TableJdbcUpsertOutputFormat(
					new SimpleJdbcConnectionProvider(options),
					dml,
					executionOptionsBuilder.build());
			} else {
				// warn: don't close over builder fields
				String sql = options.getDialect().getInsertIntoStatement(dml.getTableName(), dml.getFieldNames());
				JdbcDialect dialect = options.getDialect();
				return new JdbcBatchingOutputFormat<>(
					new SimpleJdbcConnectionProvider(options),
					executionOptionsBuilder.build(),
					ctx -> createSimpleRowExecutor(dialect, sql, dml.getFieldTypes(), ctx.getExecutionConfig().isObjectReuseEnabled()),
					tuple2 -> {
						Preconditions.checkArgument(tuple2.f0);
						return tuple2.f1;
					});
			}
		}
	}

	static JdbcBatchStatementExecutor<Row> createSimpleRowExecutor(JdbcDialect dialect, String sql, JdbcDataType[] fieldTypes, boolean objectReuse) {
		return JdbcBatchStatementExecutor.simple(sql, createRowJdbcStatementBuilder(dialect, fieldTypes), objectReuse ? Row::copy : Function.identity());
	}

	/**
	 * Creates a {@link JdbcStatementBuilder} for {@link Row} using the provided JDBC types array.
	 * Uses {@link JdbcDialect#getOutputConverter(JdbcDataType[])}
	 */
	static JdbcStatementBuilder<Row> createRowJdbcStatementBuilder(JdbcDialect dialect, JdbcDataType[] types) {
		return (st, record) ->
			dialect.getOutputConverter(types).toExternal(record, st);
	}

}
