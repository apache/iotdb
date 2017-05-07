package cn.edu.thu.tsfiledb.engine.bufferwrite;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.timeseries.write.WriteSupport;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;

/**
 * @author kangrong
 * @author liukun
 *
 */
public class TSRecordWriterParameter {
	public final FileSchema fileSchema;
	public final TSRandomAccessFileWriter outputStream;
	public final WriteSupport<TSRecord> writeSupport;

	public TSRecordWriterParameter(FileSchema fileSchema, TSRandomAccessFileWriter outputStream,
			WriteSupport<TSRecord> writeSupport) {
		this.fileSchema = fileSchema;
		this.outputStream = outputStream;
		this.writeSupport = writeSupport;
	}

}
