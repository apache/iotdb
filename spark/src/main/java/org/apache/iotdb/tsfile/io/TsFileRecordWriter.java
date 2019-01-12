package org.apache.iotdb.tsfile.io;

import org.apache.iotdb.tsfile.timeseries.basis.TsFile;
import org.apache.iotdb.tsfile.write.exception.WriteProcessException;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class TsFileRecordWriter extends RecordWriter<NullWritable, TSRecord> {

	private TsFile tsFile = null;

	public TsFileRecordWriter(TaskAttemptContext job, Path file, FileSchema fileSchema) throws IOException, WriteProcessException {
		HDFSOutputStream hdfsOutputStream = new HDFSOutputStream(file.toString(), job.getConfiguration(), false);
		tsFile = new TsFile(hdfsOutputStream, fileSchema);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException {
		tsFile.close();
	}

	@Override
	public synchronized void write(NullWritable arg0, TSRecord tsRecord) throws IOException {
		try {
			tsFile.writeRecord(tsRecord);
		} catch (WriteProcessException e) {
			e.printStackTrace();
		}
	}

}
