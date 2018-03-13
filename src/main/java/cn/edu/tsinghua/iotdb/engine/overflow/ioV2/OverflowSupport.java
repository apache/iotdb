package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import java.util.HashMap;
import java.util.Map;

import cn.edu.tsinghua.iotdb.engine.memtable.IMemSeries;
import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.iotdb.engine.memtable.PrimitiveMemTable;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

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
			memTable.write(tsRecord.deltaObjectId, dataPoint.getMeasurementId(), dataPoint.getType(), tsRecord.time,
					dataPoint.getValue().toString());
		}
	}

	public void update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType dataType,
			byte[] value) {
		if (!indexTrees.containsKey(deltaObjectId)) {
			indexTrees.put(deltaObjectId, new HashMap<>());
		}
		if (!indexTrees.get(deltaObjectId).containsKey(measurementId)) {
			indexTrees.get(deltaObjectId).put(measurementId, new OverflowSeriesImpl(measurementId, dataType));
		}
		indexTrees.get(deltaObjectId).get(measurementId).update(startTime, endTime, value);
	}

	public void delete(String deltaObjectId, String measurementId, long timestamp, TSDataType dataType) {
		if (!indexTrees.containsKey(deltaObjectId)) {
			indexTrees.put(deltaObjectId, new HashMap<>());
		}
		if (!indexTrees.get(deltaObjectId).containsKey(measurementId)) {
			indexTrees.get(deltaObjectId).put(measurementId, new OverflowSeriesImpl(measurementId, dataType));
		}
		indexTrees.get(deltaObjectId).get(measurementId).delete(timestamp);
	}

	public IMemSeries queryOverflowInsertInMemory(String deltaObjectId, String measurementId,
												  TSDataType dataType) {
		return memTable.query(deltaObjectId, measurementId, dataType);
	}

	public DynamicOneColumnData queryOverflowUpdateInMemory(String deltaObjectId, String measurementId,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, TSDataType dataType, DynamicOneColumnData data) {
		if (indexTrees.containsKey(deltaObjectId)) {
			if (indexTrees.get(deltaObjectId).containsKey(measurementId)
					&& indexTrees.get(deltaObjectId).get(measurementId).getDataType().equals(dataType)) {
				return indexTrees.get(deltaObjectId).get(measurementId).query(timeFilter, freqFilter, valueFilter,
						dataType, data);
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
