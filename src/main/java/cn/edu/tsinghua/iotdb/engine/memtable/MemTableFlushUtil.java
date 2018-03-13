package cn.edu.tsinghua.iotdb.engine.memtable;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.series.SeriesWriterImpl;

public class MemTableFlushUtil {
	private static final Logger logger = LoggerFactory.getLogger(MemTableFlushUtil.class);
	private static final int pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().pageSizeInByte;

	private static int writeOneSeries(List<TimeValuePair> tvPairs, SeriesWriterImpl seriesWriterImpl,
			TSDataType dataType) throws IOException {
		int count = 0;
		switch (dataType) {
		case BOOLEAN:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
			}
			break;
		case INT32:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
			}
			break;
		case INT64:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
			}
			break;
		case FLOAT:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
			}
			break;
		case DOUBLE:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
			}
			break;
		case TEXT:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
			}
			break;
		default:
			logger.error("don't support data type: {}", dataType);
			break;
		}
		return count;
	}

	public static void flushMemTable(FileSchema fileSchema, TsFileIOWriter tsFileIOWriter, IMemTable iMemTable)
			throws IOException {
		for (String deltaObjectId : iMemTable.getMemTableMap().keySet()) {
			long startPos = tsFileIOWriter.getPos();
			long recordCount = 0;
			tsFileIOWriter.startRowGroup(deltaObjectId);
			for (String measurementId : iMemTable.getMemTableMap().get(deltaObjectId).keySet()) {
				IMemSeries series = iMemTable.getMemTableMap().get(deltaObjectId).get(measurementId);
				MeasurementDescriptor desc = fileSchema.getMeasurementDescriptor(measurementId);
				IPageWriter pageWriter = new PageWriterImpl(desc);
				SeriesWriterImpl seriesWriter = new SeriesWriterImpl(deltaObjectId, desc, pageWriter,
						pageSizeThreshold);
				recordCount += writeOneSeries(series.getSortedTimeValuePairList(), seriesWriter, desc.getType());
				seriesWriter.writeToFileWriter(tsFileIOWriter);
			}
			long memSize = tsFileIOWriter.getPos() - startPos;
			tsFileIOWriter.endRowGroup(memSize, recordCount);
		}
	}
}
