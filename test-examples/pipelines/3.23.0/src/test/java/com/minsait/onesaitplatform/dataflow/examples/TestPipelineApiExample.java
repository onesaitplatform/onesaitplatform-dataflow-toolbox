/*
 * Copyright 2023 Indra Soluciones Tecnologías de la Información, S.L.U.
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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.model.HeaderJson;
import com.streamsets.datacollector.client.model.PreviewOutputJson;
import com.streamsets.datacollector.client.model.RecordJson;
import com.streamsets.datacollector.client.model.StageOutputJson;
import com.streamsets.pipeline.api.Field;

@Testcontainers
public class TestPipelineApiExample {

	private static final String IMAGE = "registry.onesaitplatform.com/onesaitplatform/streamsets:4.1.0-overlord-323";
	private static final int PORT = 18630;

	// In this example we use the basic image for dataflow. For your own tests,
	// create an image with all the libraries and
	// pipelines to test created. The easiest way to create the image is startup
	// dataflow, config all the resources and create
	// a new image with the command `docker commit <container>
	// <image_for_test_name>`
	// With this annotation a new contanier is created for each test, take it into
	// account for the performance of the execution.
	@Container
	public static GenericContainer<?> dataflow = new GenericContainer<>(IMAGE).withExposedPorts(PORT)
			.withStartupTimeout(Duration.ofSeconds(120));

	@Test
	public void testPipelineAPI() throws ApiException, InterruptedException, IOException {
		ApiClient apiClient = ApiUtils.newApiClient(dataflow.getMappedPort(PORT));
		
		//This example assumes that pipelines files are in src/test/resources directory.
		ApiUtils.importPipeline(apiClient, "Test.json", "Test", "0");
		PreviewOutputJson previewData = ApiUtils.runPriview(apiClient, "Test", "0", 1, 1, false);

		List<List<StageOutputJson>> batchesOutput = previewData.getBatchesOutput();

		// This test only use one batch.
		List<StageOutputJson> stageOutputs = batchesOutput.get(0);

		for (StageOutputJson stageOutput : stageOutputs) {
			// Assert status of each stage for the selected batch.
			switch (stageOutput.getInstanceName()) {
			case "DevRawDataSource_01":
				Map<String, List<RecordJson>> outputs = stageOutput.getOutput();
				for (List<RecordJson> records : outputs.values()) {
					for (RecordJson record : records) {

						// If needed, test record header values
						// Dummy test as example
						HeaderJson header = record.getHeader();
						assertTrue(header.getSourceId().equals("rawData::0"));

						Field field = ApiUtils.getFieldFromRecord(record, apiClient.getJson());
						Map<String, Field> root = field.getValueAsMap();

						// We can test any data of the records, including the data type.
						assertTrue(root.get("value1").getType().equals(Field.Type.STRING));
						assertTrue(root.get("value1").getValueAsString().equals("abc"));
						assertTrue(root.get("value2").getType().equals(Field.Type.STRING));
						assertTrue(root.get("value2").getValueAsString().equals("xyz"));
						assertTrue(root.get("value3").getType().equals(Field.Type.STRING));
						assertTrue(root.get("value3").getValueAsString().equals("lmn"));
					}
				}
				break;
			case "Trash_01":
				// trash does not have output
				break;
			default:
				// stage not tested
				break;
			}
		}
	}
}
