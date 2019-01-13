/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
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
