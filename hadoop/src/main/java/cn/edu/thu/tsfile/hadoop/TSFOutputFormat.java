package cn.edu.thu.tsfile.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSFOutputFormat extends FileOutputFormat<NullWritable, TSRow> {

	private static final Logger LOGGER = LoggerFactory.getLogger(TSFOutputFormat.class);

	public static final String FILE_SCHEMA = "tsfile.schema";

	private static final String extension = "tsfile";

	public static void setWriterSchema(Job job, JSONObject schema) {

		LOGGER.info("Set the write schema - {}", schema.toString());

		job.getConfiguration().set(FILE_SCHEMA, schema.toString());
	}

	public static void setWriterSchema(Job job, String schema) {

		LOGGER.info("Set the write schema - {}", schema);

		job.getConfiguration().set(FILE_SCHEMA, schema);
	}

	public static JSONObject getWriterSchema(JobContext jobContext) throws InterruptedException {

		String schema = jobContext.getConfiguration().get(FILE_SCHEMA);
		if (schema == null || schema == "") {
			throw new InterruptedException("The tsfile schema is null or empty");
		}
		JSONObject jsonSchema = new JSONObject(schema);

		return jsonSchema;
	}

	@Override
	public RecordWriter<NullWritable, TSRow> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {

		Path outputPath = getDefaultWorkFile(job, extension);
		LOGGER.info("The task attempt id is {}, the output path is {}", job.getTaskAttemptID(), outputPath);
		JSONObject schema = getWriterSchema(job);
		return new TSFRecordWriter(outputPath, schema);
	}
}
