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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnMapping;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.processor.jdbclookup.JdbcLookupDProcessor;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;
import com.streamsets.pipeline.stage.processor.kv.EvictionPolicyType;

public class TestJdbcProcessor {

	private static Connection connection = null;

	private static final String username = "sa";
	private static final String password = "sa";
	private static final String database = "test";
	private static final String schema = "TEST";
	private static final String tableName = "TEST_TABLE";
	private static final String fullTableName = schema+"."+tableName;
	private static final String h2ConnectionString = "jdbc:h2:mem:" + database;

	private static final String listQuery = "SELECT P_ID FROM "+ fullTableName
			+ " WHERE FIRST_NAME = '${record:value(\"[0]\")}'" + "   AND LAST_NAME = '${record:value(\"[1]\")}'";

	@BeforeAll
	public static void setUp() throws SQLException {
		// Create a table in H2 and put some data in it for querying.
		connection = DriverManager.getConnection(h2ConnectionString, username, password);
		try (Statement statement = connection.createStatement()) {
			// Setup table
			statement.addBatch("CREATE SCHEMA IF NOT EXISTS "+schema+";");
			statement.addBatch("CREATE TABLE IF NOT EXISTS "+fullTableName+" "
					+ "(P_ID INT NOT NULL PRIMARY KEY, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255));");
			statement.addBatch("INSERT INTO "+fullTableName+" VALUES (1, 'Adam', 'Kunicki')");
			statement.addBatch("INSERT INTO "+fullTableName+" VALUES (2, 'Jon', 'Natkins')");
			statement.addBatch("INSERT INTO "+fullTableName+" VALUES (3, 'Jon', 'Daulton')");
			statement.addBatch("INSERT INTO "+fullTableName+" VALUES (4, 'Girish', 'Pancha')");
			statement.addBatch("INSERT INTO "+fullTableName+" VALUES (5, 'Girish', 'Pancha')");

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

	private JdbcLookupDProcessor createProcessor() {
		JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
		processor.hikariConfigBean = new HikariPoolConfigBean();
		processor.hikariConfigBean.connectionString = h2ConnectionString;
		processor.hikariConfigBean.useCredentials = true;
		processor.hikariConfigBean.username = () -> username;
		processor.hikariConfigBean.password = () -> password;
		processor.cacheConfig = new CacheConfig();
		processor.cacheConfig.evictionPolicyType = EvictionPolicyType.EXPIRE_AFTER_WRITE;

		return processor;
	}

	@Test
	public void testSingleRecordList() throws Exception {
		List<JdbcFieldColumnMapping> columnMappings = Collections
				.unmodifiableList(Arrays.asList(new JdbcFieldColumnMapping("P_ID", "[2]")));

		JdbcLookupDProcessor processor = createProcessor();

		ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
				.addConfiguration("query", listQuery).addConfiguration("columnMappings", columnMappings)
				.addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
				.addConfiguration("missingValuesBehavior", MissingValuesBehavior.SEND_TO_ERROR)
				.addConfiguration("maxClobSize", 1000).addConfiguration("maxBlobSize", 1000)
				.addOutputLane("lane").build();

		Record record = RecordCreator.create();
		List<Field> fields = new ArrayList<>();
		fields.add(Field.create("Adam"));
		fields.add(Field.create("Kunicki"));
		record.set(Field.create(fields));

		List<Record> singleRecord = Collections.unmodifiableList(Arrays.asList(record));
		processorRunner.runInit();
		try {
			StageRunner.Output output = processorRunner.runProcess(singleRecord);
			assertEquals(1, output.getRecords().get("lane").size());

			record = output.getRecords().get("lane").get(0);

			assertNotEquals(null, record.get("[2]"));
			assertEquals(1, record.get("[2]").getValueAsInteger());
		} finally {
			processorRunner.runDestroy();
		}
	}
}
