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

public class LiveSegmentTrafficDataPipeline {

	private static final String PUB_SUB_TOPIC = "projects/extended-argon-213022/topics/Chicago_Traffic_Segment_Live";

	private static final String BIG_QUERY_TABLE = "extended-argon-213022:chicago_traffic_data.traffic_by_segment_live";

	private static final Logger LOG = LoggerFactory.getLogger(LiveRegionalTrafficDataPipeline.class);

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		options.setJobName("livesegmenttrafficdata");
		run(options);
	}

	private static void run(PipelineOptions options) {
		Pipeline pipeline = Pipeline.create(options);

		PCollection<LiveSegmentTrafficData> currentConditions = pipeline
				.apply("GetMessages", PubsubIO.readStrings().fromTopic(PUB_SUB_TOPIC))
				.apply("ExtractData", ParDo.of(new DoFn<String, LiveSegmentTrafficData>() {

					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						String line = c.element();
						LiveSegmentTrafficData data = LiveSegmentTrafficData.fromString(line);
						LOG.debug("Created segment traffic data {}", data);
						c.output(data);
					}
				}));

		currentConditions.apply("ToBQRows", ParDo.of(new DoFn<LiveSegmentTrafficData, TableRow>() {

			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
				TableRow row = new TableRow();
				LiveSegmentTrafficData trafficData = c.element();
				row.set("direction", trafficData.getDirection());
				row.set("from_street", trafficData.getFromStreet());
				row.set("last_updt", trafficData.getLastUpdate());
				row.set("segment_id", trafficData.getSegmentId());
				row.set("street", trafficData.getStreet());
				row.set("to_street", trafficData.getToStreet());
				row.set("length", trafficData.getLength());
				row.set("street_heading", trafficData.getStreetHeading());
				row.set("comments", trafficData.getComments());
				row.set("traffic", trafficData.getTraffic());
				row.set("start_longitude", trafficData.getStartLongitude());
				row.set("start_latitude", trafficData.getStartLatitude());
				row.set("end_longitude", trafficData.getEndLongitude());
				row.set("end_latitude", trafficData.getEndLatitude());
				LOG.debug("Created big query row {}", row);
				c.output(row);
			}
		})).apply(BigQueryIO.writeTableRows().to(BIG_QUERY_TABLE).withSchema(getTableSchema())
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		pipeline.run();
	}

	private static final TableSchema getTableSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("direction").setType("STRING"));
		fields.add(new TableFieldSchema().setName("from_street").setType("STRING"));
		fields.add(new TableFieldSchema().setName("last_updt").setType("DATETIME"));
		fields.add(new TableFieldSchema().setName("segment_id").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("street").setType("STRING"));
		fields.add(new TableFieldSchema().setName("to_street").setType("STRING"));
		fields.add(new TableFieldSchema().setName("length").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("street_heading").setType("STRING"));
		fields.add(new TableFieldSchema().setName("comments").setType("STRING"));
		fields.add(new TableFieldSchema().setName("traffic").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("start_longitude").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("start_latitude").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("end_longitude").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("end_latitude").setType("FLOAT"));

		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}
}
