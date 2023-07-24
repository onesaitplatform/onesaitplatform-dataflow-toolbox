/*
 * Copyright 2022 Indra Soluciones Tecnologías de la Información, S.L.U.
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.joda.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JDBCOperationType;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnParamMapping;
import com.streamsets.pipeline.lib.jdbc.JdbcHikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcMultiRowRecordWriter;
import com.streamsets.pipeline.lib.jdbc.connection.JdbcConnection;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.jdbc.JdbcDTarget;
import com.streamsets.pipeline.stage.destination.jdbc.JdbcTarget;

public class TestJdbcDestination {

	private static Connection connection = null;
	private static final String username = "sa";
	private static final String password = "sa";
	private static final String database = "test";
	private static final String schema = "TEST";
	private static final String tableName = "TEST_TABLE";
	private static final String fullTableName = schema+"."+tableName;

	private static final String h2ConnectionString = "jdbc:h2:mem:" + database;

	@BeforeAll
	public static void setUp() throws SQLException {
		final String sdcId = "testingID";
		Utils.setSdcIdCallable(new Callable<String>() {
			@Override
			public String call() {
				return sdcId;
			}
		});

		connection = DriverManager.getConnection(h2ConnectionString, username, password);

		try (Statement statement = connection.createStatement()) {
			statement.addBatch("CREATE SCHEMA IF NOT EXISTS " + schema + ";");
			statement.addBatch("CREATE TABLE IF NOT EXISTS " + fullTableName
					+ " (P_ID INT NOT NULL, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), TS TIMESTAMP, UNIQUE(P_ID), "
					+ "PRIMARY KEY(P_ID));");
			statement.executeBatch();
		}
	}

	@AfterAll
	public static void tearDown() throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute("DROP TABLE IF EXISTS " + fullTableName + ";");
			statement.execute("DROP SCHEMA IF EXISTS "+schema+";");
		}

		// Last open connection terminates H2
		connection.close();
	}

	private JdbcHikariPoolConfigBean createConfigBean(String connectionString, String username, String password) {
		JdbcHikariPoolConfigBean bean = new JdbcHikariPoolConfigBean();
		bean.connection = new JdbcConnection();
		bean.connection.connectionString = connectionString;
		bean.connection.useCredentials = true;
		bean.connection.username = () -> username;
		bean.connection.password = () -> password;

		return bean;
	}

	@Test
	public void testSingleRecord() throws Exception {
		List<JdbcFieldColumnParamMapping> fieldMappings = Collections.unmodifiableList(Arrays.asList(
				new JdbcFieldColumnParamMapping("[0]", "P_ID"), 
				new JdbcFieldColumnParamMapping("[1]", "FIRST_NAME"),
				new JdbcFieldColumnParamMapping("[2]", "LAST_NAME"), 
				new JdbcFieldColumnParamMapping("[3]", "TS")
				));

		Target target = new JdbcTarget(
				schema, 
				tableName, 
				fieldMappings, 
				false, 
				false, 
				false,
				JdbcMultiRowRecordWriter.UNLIMITED_PARAMETERS, 
				ChangeLogFormat.NONE, 
				JDBCOperationType.INSERT,
				UnsupportedOperationAction.DISCARD, 
				createConfigBean(h2ConnectionString, username, password),
				Collections.emptyList());
		TargetRunner targetRunner = new TargetRunner.Builder(JdbcDTarget.class, target).build();

		Record record = RecordCreator.create();
		List<Field> fields = new ArrayList<>();
		fields.add(Field.create(1));
		fields.add(Field.create("Adam"));
		fields.add(Field.create("Kunicki"));
		fields.add(Field.createDatetime(new Instant().toDate()));
		record.set(Field.create(fields));

		List<Record> singleRecord = Collections.unmodifiableList(Arrays.asList(record));
		targetRunner.runInit();
		targetRunner.runWrite(singleRecord);

		connection = DriverManager.getConnection(h2ConnectionString, username, password);
		try (Statement statement = connection.createStatement()) {
			ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + fullTableName);
			rs.next();
			assertEquals(1, rs.getInt(1));
		}
	}
}
