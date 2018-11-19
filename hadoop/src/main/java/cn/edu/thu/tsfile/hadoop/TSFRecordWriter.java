package cn.edu.thu.tsfile.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.hadoop.io.HDFSOutputStream;
import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.InvalidJsonSchemaException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class TSFRecordWriter extends RecordWriter<NullWritable, TSRow> {

	private static final Logger LOGGER = LoggerFactory.getLogger(TSFRecordWriter.class);

	private TsFile write = null;

	public TSFRecordWriter(Path path, JSONObject schema) throws InterruptedException, IOException {
		// construct the internalrecordwriter
		FileSchema fileSchema = null;
		try {
			fileSchema = new FileSchema(schema);
		} catch (InvalidJsonSchemaException e) {
			e.printStackTrace();
			LOGGER.error("Construct the tsfile schema failed, the reason is {}", e.getMessage());
			throw new InterruptedException(e.getMessage());
		}

		HDFSOutputStream hdfsOutputStream = new HDFSOutputStream(path, new Configuration(), false);
		try {
			write = new TsFile(hdfsOutputStream, fileSchema);
		} catch (WriteProcessException e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public void write(NullWritable key, TSRow value) throws IOException, InterruptedException {

		try {
			write.writeRecord(value.getRow());
		} catch (WriteProcessException e) {
			e.printStackTrace();
			LOGGER.error("Write tsfile record error, the error message is {}", e.getMessage());
			throw new InterruptedException(e.getMessage());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {

		LOGGER.info("Close the recordwriter, the task attempt id is {}", context.getTaskAttemptID());
		write.close();
	}

}
