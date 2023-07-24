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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.JSON;
import com.streamsets.datacollector.client.TypeRef;
import com.streamsets.datacollector.client.api.PreviewApi;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.model.PipelineEnvelopeJson;
import com.streamsets.datacollector.client.model.PreviewInfoJson;
import com.streamsets.datacollector.client.model.PreviewInfoJson.StatusEnum;
import com.streamsets.datacollector.client.model.PreviewOutputJson;
import com.streamsets.datacollector.client.model.RecordJson;
import com.streamsets.datacollector.client.model.StageOutputJson;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.record.FieldDeserializer;
import com.streamsets.datacollector.restapi.bean.FieldJson;
import com.streamsets.pipeline.api.Field;

public class ApiUtils {

	public static String getUrl(int port) {
		return "http://127.0.0.1:" + port;
	}

	private static final String user = "admin";
	private static final String password = "admin";
	private static final String authType = "basic";

	public static ApiClient newApiClient(int port) {
		ApiClient apiClient = new ApiClient(authType);
		apiClient.setUserAgent("SDC CLI");
		apiClient.setBasePath(getUrl(port) + "/rest");
		apiClient.setUsername(user);
		apiClient.setPassword(password);
		
		SimpleModule module = new SimpleModule();
		module.addDeserializer(FieldJson.class, new FieldDeserializer());
		apiClient.getJson().getMapper().registerModule(module);
		
		return apiClient;
	}

	private static String getPipelinePath(String fileName) {
		Path resourceDirectory = Paths.get("src","test","resources", fileName);
		String absolutePath = resourceDirectory.toFile().getAbsolutePath();
		return absolutePath;
	}

	public static PipelineEnvelopeJson importPipeline(ApiClient apiClient, String filename, String pipelineId, String rev) throws ApiException {
		TypeRef<PipelineEnvelopeJson> returnType = new TypeRef<PipelineEnvelopeJson>() {};
		PipelineEnvelopeJson pipelineEnvelopeJson = apiClient.getJson().deserialize(new File(getPipelinePath(filename)), returnType);

		StoreApi storeApi = new StoreApi(apiClient);
		PipelineEnvelopeJson importPipeline = storeApi.importPipeline(
				pipelineId, 
				rev, 
				false, 
				false, 
				false, 
				false, 
				pipelineEnvelopeJson);
		return importPipeline;
	}

	public static PreviewOutputJson runPriview(ApiClient apiClient, String pipelineId, String rev, int batchSize, int batches, boolean writeTarget) throws ApiException, InterruptedException {
		PreviewApi previewApi = new PreviewApi(apiClient);

		PreviewInfoJson previewOutput = previewApi.previewWithOverride(
				pipelineId, 
				Collections.<StageOutputJson>emptyList(), 
				rev, 
				batchSize, 
				batches, 
				writeTarget, 
				null, 
				null);
		String previewerId = previewOutput.getPreviewerId();

		PreviewStatus status = null;
		do {
			PreviewInfoJson previewStatus = previewApi.getPreviewStatus(pipelineId, previewerId);
			TimeUnit.SECONDS.sleep(5);
			StatusEnum statusEnum = previewStatus.getStatus();
			status = PreviewStatus.valueOf(statusEnum.name());
		} while (status.isActive());

		assertTrue(PreviewStatus.FINISHED.equals(status));

		PreviewOutputJson previewData = previewApi.getPreviewData(pipelineId, previewerId); 
		return previewData;
	}

	public static Field getFieldFromRecord(RecordJson record, JSON json) throws ApiException {
		Object value = record.getValue();
		return getFieldFromObject(value, json);
	}

	private static Field getFieldFromObject(Object value, JSON json) throws ApiException {
		String valueJson = json.serialize(value);
		FieldJson field = json.deserialize(valueJson, new TypeRef<FieldJson>() {});
		return field.getField();
	}
}
