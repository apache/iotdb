package org.apache.iotdb.db.engine.overflow.ioV2;

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to store and query all overflow data in memory.<br>
 * This just represent someone storage group.<br>
 * 
 * @author liukun
 *
 */
public class OverflowSupport {

	/**
	 * store update and delete data
	 */
	private Map<String, Map<String, OverflowSeriesImpl>> indexTrees;

	/**
	 * store insert data
	 */
	private IMemTable memTable;

	public OverflowSupport() {
		indexTrees = new HashMap<>();
		//memTable = new TreeSetMemTable();
		memTable = new PrimitiveMemTable();
	}

	public void insert(TSRecord tsRecord) {
		for (DataPoint dataPoint : tsRecord.dataPointList) {
			memTable.write(tsRecord.deviceId, dataPoint.getMeasurementId(), dataPoint.getType(), tsRecord.time,
					dataPoint.getValue().toString());
		}
	}
	@Deprecated
	public void update(String deviceId, String measurementId, long startTime, long endTime, TSDataType dataType,
			byte[] value) {
		if (!indexTrees.containsKey(deviceId)) {
			indexTrees.put(deviceId, new HashMap<>());
		}
		if (!indexTrees.get(deviceId).containsKey(measurementId)) {
			indexTrees.get(deviceId).put(measurementId, new OverflowSeriesImpl(measurementId, dataType));
		}
		indexTrees.get(deviceId).get(measurementId).update(startTime, endTime, value);
	}
	@Deprecated
	public void delete(String deviceId, String measurementId, long timestamp, TSDataType dataType) {
		if (!indexTrees.containsKey(deviceId)) {
			indexTrees.put(deviceId, new HashMap<>());
		}
		if (!indexTrees.get(deviceId).containsKey(measurementId)) {
			indexTrees.get(deviceId).put(measurementId, new OverflowSeriesImpl(measurementId, dataType));
		}
		indexTrees.get(deviceId).get(measurementId).delete(timestamp);
	}

	public TimeValuePairSorter queryOverflowInsertInMemory(String deviceId, String measurementId,
														   TSDataType dataType) {
		return memTable.query(deviceId, measurementId, dataType);
	}

	public BatchData queryOverflowUpdateInMemory(String deviceId, String measurementId,
												 TSDataType dataType, BatchData data) {
		if (indexTrees.containsKey(deviceId)) {
			if (indexTrees.get(deviceId).containsKey(measurementId)
					&& indexTrees.get(deviceId).get(measurementId).getDataType().equals(dataType)) {
				return indexTrees.get(deviceId).get(measurementId).query(data);
			}
		}
		return null;
	}

	public boolean isEmptyOfOverflowSeriesMap() {
		return indexTrees.isEmpty();
	}

	public Map<String, Map<String, OverflowSeriesImpl>> getOverflowSeriesMap() {
		return indexTrees;
	}

	public boolean isEmptyOfMemTable() {
		return memTable.isEmpty();
	}

	public IMemTable getMemTabale() {
		return memTable;
	}

	public long getSize() {
		// memtable+overflowTreesMap
		// TODO: calculate the size of this overflow support
		return 0;
	}

	public void clear() {
		indexTrees.clear();
		memTable.clear();
	}
}
