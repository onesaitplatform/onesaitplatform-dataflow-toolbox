/*
 * Copyright 2023 Indra Soluciones Tecnologías de la Información, S.L.U.
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.minsait.onesaitplatform.dataflow.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.JdbcDSource;
import com.streamsets.pipeline.stage.origin.jdbc.JdbcRecordType;
import com.streamsets.pipeline.stage.origin.jdbc.JdbcSource;

public class TestJdbcSource {

	private static final int BATCH_SIZE = 1000;
	private static final int CLOB_SIZE = 1000;

	private static Connection connection = null;

	private static final String username = "sa";
	private static final String password = "sa";
	private static final String database = "test";
	private static final String schema = "TEST";
	private static final String tableName = "TEST_TABLE";
	private static final String fullTableName = schema+"."+tableName;
	private static final String h2ConnectionString = "jdbc:h2:mem:" + database;

	private static final String query = "SELECT * FROM "+fullTableName+" WHERE P_ID > ${offset} ORDER BY P_ID ASC LIMIT 10;";
	private static final String initialOffset = "0";
	private static final String queriesPerSecond = "0";
	private static final long queryInterval = 0;

	@BeforeAll
	public static void setUp() throws SQLException {
		final String sdcId = "testingID";
		Utils.setSdcIdCallable(new Callable<String>() {
			@Override
			public String call() {
				return sdcId;
			}
		});

		// Create a table in H2 and put some data in it for querying.
		connection = DriverManager.getConnection(h2ConnectionString, username, password);
		try (Statement statement = connection.createStatement()) {
			// Setup table
			statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");
			statement.addBatch(
					"CREATE TABLE IF NOT EXISTS "+fullTableName+" " +
							"(p_id INT NOT NULL, first_name VARCHAR(255), last_name VARCHAR(255));"
					);

			// Add some data
			statement.addBatch("INSERT INTO "+fullTableName+" VALUES (1, 'Adam', 'Kunicki')");
			statement.addBatch("INSERT INTO "+fullTableName+" VALUES (2, 'Jon', 'Natkins')");
			statement.addBatch("INSERT INTO "+fullTableName+" VALUES (3, 'Jon', 'Daulton')");
			statement.addBatch("INSERT INTO "+fullTableName+" VALUES (4, 'Girish', 'Pancha')");

			statement.executeBatch();
		}
	}

	@AfterAll
	public static void tearDown() throws SQLException {
		try (Statement statement = connection.createStatement()) {
			// Setup table
			statement.execute("DROP TABLE IF EXISTS "+fullTableName+";");
			statement.execute("DROP SCHEMA IF EXISTS "+schema+";");
		}

		// Last open connection terminates H2
		connection.close();
	}

	private HikariPoolConfigBean createConfigBean(String connectionString, String username, String password) {
		HikariPoolConfigBean bean = new HikariPoolConfigBean();
		bean.connectionString = connectionString;
		bean.useCredentials = true;
		bean.username = () -> username;
		bean.password = () -> password;

		return bean;
	}

	private void runInsertNewRows() throws SQLException {
		try (Connection connection = DriverManager.getConnection(h2ConnectionString, username, password)) {
			try (Statement statement = connection.createStatement()) {
				// Add some data
				statement.addBatch("INSERT INTO "+fullTableName+" VALUES (9, 'Arvind', 'Prabhakar')");
				statement.addBatch("INSERT INTO "+fullTableName+" VALUES (10, 'Brock', 'Noland')");

				statement.executeBatch();
			}
		}
	}

	private void runInsertOldRows() throws SQLException {
		try (Connection connection = DriverManager.getConnection(h2ConnectionString, username, password)) {
			try (Statement statement = connection.createStatement()) {
				// Add some data
				statement.addBatch("INSERT INTO "+fullTableName+" VALUES (5, 'Arvind', 'Prabhakar')");
				statement.addBatch("INSERT INTO "+fullTableName+" VALUES (6, 'Brock', 'Noland')");

				statement.executeBatch();
			}
		}
	}

	@Test
	public void testIncrementalMode() throws Exception {
		JdbcSource origin = new JdbcSource(
				true,
				query,
				initialOffset,
				"P_ID",
				false,
				"",
				1000,
				JdbcRecordType.LIST_MAP,
				new CommonSourceConfigBean(queriesPerSecond, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
				false,
				"",
				createConfigBean(h2ConnectionString, username, password),
				UnknownTypeAction.STOP_PIPELINE,
				queryInterval
				);
		SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
				.addOutputLane("lane")
				.build();

		runner.runInit();

		try {
			// Check that existing rows are loaded.
			StageRunner.Output output = runner.runProduce(null, 2);
			Map<String, List<Record>> recordMap = output.getRecords();
			List<Record> parsedRecords = recordMap.get("lane");

			assertEquals(2, parsedRecords.size());

			assertEquals("2", output.getNewOffset());

			// Check that the remaining rows in the initial cursor are read.
			output = runner.runProduce(output.getNewOffset(), 100);
			parsedRecords = output.getRecords().get("lane");
			assertEquals(2, parsedRecords.size());


			// Check that new rows are loaded.
			runInsertNewRows();
			output = runner.runProduce(output.getNewOffset(), 100);
			parsedRecords = output.getRecords().get("lane");
			assertEquals(2, parsedRecords.size());

			assertEquals("10", output.getNewOffset());

			// Check that older rows are not loaded.
			runInsertOldRows();
			output = runner.runProduce(output.getNewOffset(), 100);
			parsedRecords = output.getRecords().get("lane");
			assertEquals(0, parsedRecords.size());
		} finally {
			runner.runDestroy();
		}
	}
}
