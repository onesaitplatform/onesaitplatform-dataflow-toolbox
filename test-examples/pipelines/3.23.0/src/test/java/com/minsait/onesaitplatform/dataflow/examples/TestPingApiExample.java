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

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.api.SystemApi;

@Testcontainers
public class TestPingApiExample {

	private static final int port = 18630;

	// In this example we use the basic image for dataflow. For your own tests,
	// create an image with all the libraries and
	// pipelines to test created. The easiest way to create the image is startup
	// dataflow, config all the resources and create
	// a new image with the command `docker commit <container>
	// <image_for_test_name>`
	// With this annotation a new contanier is created for each test, take it into
	// account for the performance of the execution.
	@Container
	public static GenericContainer<?> dataflow = new GenericContainer<>(
			"registry.onesaitplatform.com/onesaitplatform/streamsets:4.1.0-overlord-323").withExposedPorts(port)
			.withStartupTimeout(Duration.ofSeconds(120));

	@Test
	public void testPingAPI() throws ApiException, JsonProcessingException {
		SystemApi systemApi = new SystemApi(ApiUtils.newApiClient(dataflow.getMappedPort(port)));
		Map<String, Object> buildInfo = systemApi.getBuildInfo();

		assertTrue(buildInfo.containsKey("version"));
	}
}
