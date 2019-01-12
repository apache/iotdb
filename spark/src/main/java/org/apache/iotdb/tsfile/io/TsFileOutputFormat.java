package org.apache.iotdb.tsfile.io;

import org.apache.iotdb.tsfile.write.exception.WriteProcessException;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class TsFileOutputFormat extends FileOutputFormat<NullWritable, TSRecord> {

    private FileSchema fileSchema;

    public TsFileOutputFormat(FileSchema fileSchema) {
        this.fileSchema = fileSchema;
    }

    @Override
    public RecordWriter<NullWritable, TSRecord> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        Path path = getDefaultWorkFile(job, "");
        try {
            return new TsFileRecordWriter(job, path, fileSchema);
        } catch (WriteProcessException e) {
            e.printStackTrace();
            throw new InterruptedException("construct TsFileRecordWriter failed");
        }
    }

}
