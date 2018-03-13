package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

/**
 * @author kangrong
 *
 */
public class BufferWriteIOWriter extends TsFileIOWriter {

	/*
	 * The backup list is used to store the rowgroup's metadata whose data has
	 * been flushed into file.
	 */
	private final List<RowGroupMetaData> backUpList = new ArrayList<RowGroupMetaData>();
	private int lastRowGroupIndex = 0;

	public BufferWriteIOWriter(ITsRandomAccessFileWriter output) throws IOException {
		super(output);
	}

	/**
	 * This is just used to restore a tsfile from the middle of the file
	 * 
	 * @param schema
	 * @param output
	 * @param rowGroups
	 * @throws IOException
	 */
	public BufferWriteIOWriter(ITsRandomAccessFileWriter output, long offset, List<RowGroupMetaData> rowGroups)
			throws IOException {
		super(output, offset, rowGroups);
		addrowGroupsTobackupList(rowGroups);

	}

	private void addrowGroupsTobackupList(List<RowGroupMetaData> rowGroups) {
		for (RowGroupMetaData rowGroupMetaData : rowGroups) {
			backUpList.add(rowGroupMetaData);
		}
		lastRowGroupIndex = rowGroups.size();
	}

	/**
	 * <b>Note that</b>,the method is not thread safe.
	 */
	public void addNewRowGroupMetaDataToBackUp() {
		for (int i = lastRowGroupIndex; i < rowGroupMetaDatas.size(); i++) {
			backUpList.add(rowGroupMetaDatas.get(i));
		}
		lastRowGroupIndex = rowGroupMetaDatas.size();
	}

	/**
	 * <b>Note that</b>, the method is not thread safe. You mustn't do any
	 * change on the return.<br>
	 *
	 * @return
	 */
	public List<RowGroupMetaData> getCurrentRowGroupMetaList(String deltaObjectId) {
		List<RowGroupMetaData> ret = new ArrayList<>();
		for (RowGroupMetaData rowGroupMetaData : backUpList) {
			if (rowGroupMetaData.getDeltaObjectID().equals(deltaObjectId)) {
				ret.add(rowGroupMetaData);
			}
		}
		return ret;
	}

	public List<TimeSeriesChunkMetaData> getCurrentTimeSeriesMetadataList(String deltaObjectId, String measurementId,
			TSDataType dataType) {
		List<TimeSeriesChunkMetaData> chunkMetaDatas = new ArrayList<>();
		for (RowGroupMetaData rowGroupMetaData : backUpList) {
			if (rowGroupMetaData.getDeltaObjectID().equals(deltaObjectId)) {
				for (TimeSeriesChunkMetaData chunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
					// filter data-type and measurementId
					if (chunkMetaData.getProperties().getMeasurementUID().equals(measurementId)
							&& chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType().equals(dataType)) {
						chunkMetaDatas.add(chunkMetaData);
					}
				}
			}
		}
		return chunkMetaDatas;
	}
}
