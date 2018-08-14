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
package com.preetika.trafficdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class LiveRegionalTrafficDataPipeline {

	private static final String PUB_SUB_TOPIC = "projects/extended-argon-213022/topics/Chicago_Traffic_Region_Live";

	private static final String BIG_QUERY_TABLE = "extended-argon-213022:chicago_traffic_data.traffic_by_region_live";

	private static final Logger LOG = LoggerFactory.getLogger(LiveRegionalTrafficDataPipeline.class);

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		options.setJobName("liveregionaltrafficdata");
		run(options);
	}

	private static void run(PipelineOptions options) {
		Pipeline pipeline = Pipeline.create(options);

		PCollection<LiveRegionalTrafficData> currentConditions = pipeline
				.apply("GetMessages", PubsubIO.readStrings().fromTopic(PUB_SUB_TOPIC))
				.apply("ExtractData", ParDo.of(new DoFn<String, LiveRegionalTrafficData>() {

					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						String line = c.element();
						c.output(LiveRegionalTrafficData.fromString(line));
					}
				}));

		currentConditions.apply("ToBQRows", ParDo.of(new DoFn<LiveRegionalTrafficData, TableRow>() {

			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
				TableRow row = new TableRow();
				LiveRegionalTrafficData trafficData = c.element();
				row.set("current_speed", trafficData.getCurrentSpeed());
				row.set("east", trafficData.getEast());
				row.set("last_updt", trafficData.getLast_updt());
				row.set("region_id", trafficData.getRegionId());
				row.set("north", trafficData.getNorth());
				row.set("south", trafficData.getSouth());
				row.set("region", trafficData.getRegion());
				row.set("west", trafficData.getWest());
				row.set("description", trafficData.getDescription());
				c.output(row);
			}
		})).apply(BigQueryIO.writeTableRows().to(BIG_QUERY_TABLE).withSchema(getTableSchema())
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		pipeline.run();
	}

	private static final TableSchema getTableSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("current_speed").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("east").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("last_updt").setType("DATETIME"));
		fields.add(new TableFieldSchema().setName("region_id").setType("STRING"));
		fields.add(new TableFieldSchema().setName("north").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("south").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("region").setType("STRING"));
		fields.add(new TableFieldSchema().setName("west").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("description").setType("STRING"));
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}
}
