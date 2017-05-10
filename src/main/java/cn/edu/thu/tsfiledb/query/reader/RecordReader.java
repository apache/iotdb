package cn.edu.thu.tsfiledb.query.reader;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.aggregation.AggregationResult;
import cn.edu.thu.tsfiledb.query.management.ReadLockManager;
import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;
import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.readSupport.ColumnInfo;
import cn.edu.thu.tsfiledb.engine.filenode.QueryStructure;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfile.common.exception.ProcessorException;


/**
 * @description This class implements several read methods which can read data in different ways.<br>
 * This class provides some APIs for reading.
 * @author Jinrui Zhang
 *
 */

public class RecordReader {

	static final Logger logger = LoggerFactory.getLogger(RecordReader.class);
	private ReaderManager readerManager;
	private int lockToken;
	private String deltaObjectUID;
	private String measurementID;
	public DynamicOneColumnData insertDataInMemory;
	public List<Object> overflowInfo;
	
	/**
	 * 
	 * @param rafList
	 * @throws IOException 
	 */
	public RecordReader(List<TSRandomAccessFileReader> rafList, String deltaObjectUID, String measurementID,
			int lockToken, DynamicOneColumnData insertDataInMemory, List<Object> overflowInfo) throws IOException {
		this.readerManager = new ReaderManager(rafList);
		this.deltaObjectUID = deltaObjectUID;
		this.measurementID = measurementID;
		this.lockToken = lockToken;
		this.insertDataInMemory = insertDataInMemory;
		this.overflowInfo = overflowInfo;
	}

	/**
	 * 
	 * @param rafList
	 * @param unenvelopedFileReader
	 * @param rowGroupMetadataList
	 * @throws IOException 
	 */
	public RecordReader(List<TSRandomAccessFileReader> rafList, TSRandomAccessFileReader unenvelopedFileReader,
			List<RowGroupMetaData> rowGroupMetadataList, String deltaObjectUID, String measurementID, int lockToken,
			DynamicOneColumnData insertDataInMemory, List<Object> overflowInfo) throws IOException {
		this.readerManager = new ReaderManager(rafList, unenvelopedFileReader, rowGroupMetadataList);
		this.deltaObjectUID = deltaObjectUID;
		this.measurementID = measurementID;
		this.lockToken = lockToken;
		this.insertDataInMemory = insertDataInMemory;
		this.overflowInfo = overflowInfo;
	}

	/**
	 * Read function 1* : read one column with overflow
	 * 
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public DynamicOneColumnData getValueInOneColumnWithOverflow(String deviceUID, String sensorId,
			DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, DynamicOneColumnData insertTrue,
			SingleSeriesFilterExpression timeFilter, DynamicOneColumnData res, int fetchSize)
					throws ProcessorException, IOException {

		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deviceUID);
		int i = 0;
		if (res != null) {
			i = res.getRowGroupIndex();
		}
		for (; i < rowGroupReaderList.size(); i++) {
			RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
			res = getValueInOneColumnWithOverflow(rowGroupReader, sensorId, updateTrue, updateFalse, insertTrue,
					timeFilter, res, fetchSize);
			res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
			if (res.length >= fetchSize) {
				res.hasReadAll = false;
				return res;
			}
		}

		if (res == null) {
			res = createAOneColRetByFullPath(deviceUID + "." + sensorId);
		}
		// add left insert values
		if (insertTrue != null) {
			res.hasReadAll = addLeftInsertValue(res, insertTrue, fetchSize);
		} else {
			res.hasReadAll = true;
		}
		return res;
	}

	public DynamicOneColumnData getValueInOneColumnWithOverflow(RowGroupReader rowGroupReader, String sensorId,
			DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, DynamicOneColumnData insertTrue,
			SingleSeriesFilterExpression timeFilter, DynamicOneColumnData res, int fetchSize) throws IOException {
		DynamicOneColumnData v = rowGroupReader.getValueReaders().get(sensorId)
				.getValuesWithOverFlow(updateTrue, updateFalse, insertTrue, timeFilter, null, null, res, fetchSize);
		return v;
	}

	/**
	 * Read function 2* : read one column with filter and overflow
	 * 
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public DynamicOneColumnData getValueWithFilterAndOverflow(String deviceUID, String sensorId,
			DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, DynamicOneColumnData insertTrue,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
			DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException {

		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deviceUID);

		int i = 0;
		if (res != null) {
			i = res.getRowGroupIndex();
		}
		for (; i < rowGroupReaderList.size(); i++) {
			RowGroupReader rowGroupReader = rowGroupReaderList.get(i);

			res = getValueWithFilterAndOverflow(rowGroupReader, sensorId, updateTrue, updateFalse, insertTrue,
					timeFilter, freqFilter, valueFilter, res, fetchSize);
			res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
			if (res.length >= fetchSize) {
				res.hasReadAll = false;
				return res;
			}
		}

		if (res == null) {
			res = createAOneColRetByFullPath(deviceUID + "." + sensorId);
		}

		// add left insert values
		if (insertTrue != null) {
			res.hasReadAll = addLeftInsertValue(res, insertTrue, fetchSize);
		} else {
			res.hasReadAll = true;
		}
		return res;
	}

	public DynamicOneColumnData createAOneColRetByFullPath(String fullPath) throws ProcessorException {
		try {
			TSDataType type = MManager.getInstance().getSeriesType(fullPath);
			DynamicOneColumnData res = new DynamicOneColumnData(type, true);
			res.setDeltaObjectType(MManager.getInstance().getDeltaObjectTypeByPath(fullPath));
			return res;
		} catch (PathErrorException e) {
			// TODO Auto-generated catch block
			throw new ProcessorException(e.getMessage());
		}
	}

	public DynamicOneColumnData getValueWithFilterAndOverflow(RowGroupReader rowGroupReader, String sensorId,
			DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, DynamicOneColumnData insertTrue,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
			DynamicOneColumnData res, int fetchSize) throws IOException {
		DynamicOneColumnData v = rowGroupReader.getValueReaders().get(sensorId)
				.getValuesWithOverFlow(updateTrue, updateFalse, insertTrue, timeFilter, freqFilter, valueFilter, res,
						fetchSize);
		return v;
	}
	
	public AggregationResult aggregate(String deviceUID, String sensorId, AggregateFunction func,
			DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, DynamicOneColumnData insertTrue,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter
			) throws ProcessorException, IOException {
		
		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deviceUID);
		
		for (int i = 0; i < rowGroupReaderList.size(); i++) {
			RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
			aggregate(rowGroupReader, sensorId, func, insertTrue, updateTrue, updateFalse
					, timeFilter, freqFilter, valueFilter);
		}
		// add left insert values
		if (insertTrue != null && insertTrue.curIdx < insertTrue.length) {
			func.calculateFromDataInThisPage(insertTrue.sub(insertTrue.curIdx));
		}
		return func.result;
	}

	
	public AggregationResult aggregate(RowGroupReader rowGroupReader, String sensorId, AggregateFunction func
			, DynamicOneColumnData insertTrue, DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse
			, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException{
		AggregationResult aggrRet = rowGroupReader.getValueReaders().get(sensorId)
				.aggreate(func, insertTrue, updateTrue, updateFalse, timeFilter, freqFilter, valueFilter);
		return aggrRet;
	}

	
	/**
	 * function 3* Get time value according to filter for one column
	 * 
	 * @return
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public DynamicOneColumnData getTimeUseValueFilterWithOverflow(SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, DynamicOneColumnData updateTrue,
			DynamicOneColumnData updateFalse, DynamicOneColumnData insertTrue, DynamicOneColumnData res, int fetchSize)
					throws ProcessorException, IOException {
		String deviceUID = valueFilter.getFilterSeries().getDeltaObjectUID();
		String sensorId = valueFilter.getFilterSeries().getMeasurementUID();

		DynamicOneColumnData v = getValueWithFilterAndOverflow(deviceUID, sensorId, updateTrue, updateFalse, insertTrue,
				timeFilter, freqFilter, valueFilter, res, fetchSize);
		return v;
		// long[] timeRet = v.getTimeAsArray();
		// return timeRet;
	}

	/**
	 * function 4#1: for cross getIndex. To get values in one column according
	 * to a time list
	 * 
	 * @param deviceUID
	 * @param sensorId
	 * @param timeRet
	 * @return
	 * @throws IOException
	 */
	public DynamicOneColumnData getValuesUseTimeValue(String deviceUID, String sensorId, long[] timeRet)
			throws IOException {
		DynamicOneColumnData res = null;
		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deviceUID);
		for (int i = 0; i < rowGroupReaderList.size(); i++) {
			RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
			if (i == 0) {
				res = getValuesUseTimeValue(rowGroupReader, sensorId, timeRet);
				res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
			} else {
				DynamicOneColumnData tmpRes = getValuesUseTimeValue(rowGroupReader, sensorId, timeRet);
				res.mergeRecord(tmpRes);
			}
		}
		return res;
	}

	/**
	 * function 4#2: for cross getIndex. To get values in one column according
	 * to a time list from specific RowGroupReader(s)
	 * 
	 * @param deviceUID
	 * @param sensorId
	 * @param timeRet
	 * @param idxs
	 * @return
	 * @throws IOException
	 */
	public DynamicOneColumnData getValuesUseTimeValue(String deviceUID, String sensorId, long[] timeRet,
			ArrayList<Integer> idxs) throws IOException {
		DynamicOneColumnData res = null;
		List<RowGroupReader> rowGroupReaderList = readerManager.getAllRowGroupReaders();

		boolean init = false;
		for (int i = 0; i < idxs.size(); i++) {
			int idx = idxs.get(i);
			RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
			if (!deviceUID.equals(rowGroupReader.getDeltaObjectUID())) {
				continue;
			}
			if (!init) {
				res = getValuesUseTimeValue(rowGroupReader, sensorId, timeRet);
				res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
				init = true;
			} else {
				DynamicOneColumnData tmpRes = getValuesUseTimeValue(rowGroupReader, sensorId, timeRet);
				res.mergeRecord(tmpRes);
			}
		}
		return res;
	}

	private DynamicOneColumnData getValuesUseTimeValue(RowGroupReader rowGroupReader, String sensor, long[] timeRet)
			throws IOException {
		return rowGroupReader.readValueUseTimeValue(sensor, timeRet);
	}
	
	/**
	 * function 4*
	 * 
	 * @return
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public DynamicOneColumnData getValuesUseTimeValueWithOverflow(String deviceUID, String sensorId, long[] timeRet,
			DynamicOneColumnData updateTrue, DynamicOneColumnData insertTrue, SingleSeriesFilterExpression deleteFilter)
					throws ProcessorException, IOException {
		TSDataType dataType;
		String deviceType;
		try {
			dataType = MManager.getInstance().getSeriesType(deviceUID + "." + sensorId);
			deviceType = MManager.getInstance().getDeltaObjectTypeByPath(deviceUID);
		} catch (PathErrorException e) {
			throw new ProcessorException(e.getMessage());
		}
		DynamicOneColumnData oldRes = getValuesUseTimeValue(deviceUID, sensorId, timeRet);
		if (oldRes == null) {
			oldRes = new DynamicOneColumnData(dataType, true);
			oldRes.setDeltaObjectType(deviceType);
		}
		DynamicOneColumnData res = new DynamicOneColumnData(dataType, true);
		res.setDeltaObjectType(deviceType);

		// timeRet中的时间点即为最终的时间点，不需要在插入新的时间点，因为该值已经包含overflow中的值
		int oldResIdx = 0;
		int insertIdx = 0;
		int updateIdx = 0;

		for (int i = 0; i < timeRet.length; i++) {
			if (oldResIdx < oldRes.timeLength && timeRet[i] == oldRes.getTime(oldResIdx)) {
				// 先看这个值是不是已经被更新了
				boolean isUpdated = false;
				while (updateTrue != null && updateIdx < updateTrue.length - 1
						&& timeRet[i] >= updateTrue.getTime(updateIdx)) {
					if (timeRet[i] <= updateTrue.getTime(updateIdx + 1)) {
						res.putAValueFromDynamicOneColumnData(updateTrue, updateIdx / 2);
						isUpdated = true;
						break;
					} else {
						updateIdx += 2;
					}
				}
				if (!isUpdated) {
					res.putAValueFromDynamicOneColumnData(oldRes, oldResIdx);
				}
				res.putTime(timeRet[i]);
				oldResIdx++;
			}

			while (insertTrue != null && insertIdx < insertTrue.timeLength
					&& insertTrue.getTime(insertIdx) <= timeRet[i]) {
				if (timeRet[i] == insertTrue.getTime(insertIdx)) {
					res.putAValueFromDynamicOneColumnData(insertTrue, insertIdx);
					res.putTime(timeRet[i]);
				}
				insertIdx++;
			}

		}

		return res;
	}

	/**
	 * Put left values in insertTrue to res.
	 * 
	 * @param res
	 * @param insertTrue
	 * @param fetchSize
	 * @return true represents that all the values has been read.
	 */
	public boolean addLeftInsertValue(DynamicOneColumnData res, DynamicOneColumnData insertTrue, int fetchSize) {
		long maxTime;
		if (res.length > 0) {
			maxTime = res.getTime(res.length - 1);
		} else {
			maxTime = -1;
		}
		// Binary Search ?
		// add left insert values
		for (int i = insertTrue.curIdx; i < insertTrue.length; i++) {
			if (maxTime < insertTrue.getTime(i)) {
				res.putTime(insertTrue.getTime(i));
				res.putAValueFromDynamicOneColumnData(insertTrue, i);
				// Don't forget to increase insertTrue.curIdx
				insertTrue.curIdx++;
			}
			// when the length reach to fetchSize, stop put values and return
			// false
			if (res.length >= fetchSize) {
				return false;
			}
		}
		return true;
	}

	public ArrayList<ColumnInfo> getAllSeries() {
		HashMap<String, Integer> seriesMap = new HashMap<>();
		ArrayList<ColumnInfo> res = new ArrayList<>();
		List<RowGroupReader> rowGroupReaders = readerManager.getAllRowGroupReaders();

		for (RowGroupReader rgr : rowGroupReaders) {
			for (String sensor : rgr.seriesTypeMap.keySet()) {
				if (!seriesMap.containsKey(sensor)) {
					res.add(new ColumnInfo(sensor, rgr.seriesTypeMap.get(sensor)));
					seriesMap.put(sensor, 1);
				}
			}
		}
		return res;
	}

	public List<RowGroupReader> getAllRowGroupReaders() {
		return readerManager.getAllRowGroupReaders();
	}

	public ArrayList<String> getAllDevice() {
		ArrayList<String> res = new ArrayList<>();
		HashMap<String, Integer> deviceMap = new HashMap<>();
		List<RowGroupReader> rowGroupReaders = readerManager.getAllRowGroupReaders();
		for (RowGroupReader rgr : rowGroupReaders) {
			String deviceUID = rgr.getDeltaObjectUID();
			if (!deviceMap.containsKey(deviceUID)) {
				res.add(deviceUID);
				deviceMap.put(deviceUID, 1);
			}
		}
		return res;
	}

	public HashMap<String, ArrayList<ColumnInfo>> getAllColumns() {
		HashMap<String, ArrayList<ColumnInfo>> res = new HashMap<>();
		HashMap<String, List<RowGroupReader>> rowGroupReaders = readerManager.getRowGroupReaderMap();
		for (String deviceUID : rowGroupReaders.keySet()) {
			HashMap<String, Integer> sensorMap = new HashMap<>();
			ArrayList<ColumnInfo> cols = new ArrayList<>();
			for (RowGroupReader rgr : rowGroupReaders.get(deviceUID)) {
				for (String sensor : rgr.seriesTypeMap.keySet()) {
					if (!sensorMap.containsKey(sensor)) {
						cols.add(new ColumnInfo(sensor, rgr.seriesTypeMap.get(sensor)));
						sensorMap.put(sensor, 1);
					}
				}
			}
			res.put(deviceUID, cols);
		}
		return res;
	}

	public HashMap<String, Integer> getDeviceRowGroupCounts() {
		HashMap<String, Integer> res = new HashMap<>();
		HashMap<String, List<RowGroupReader>> rowGroupReaders = readerManager.getRowGroupReaderMap();
		for (String deviceUID : rowGroupReaders.keySet()) {
			res.put(deviceUID, rowGroupReaders.get(deviceUID).size());
		}
		return res;
	}

	public HashMap<String, String> getDeviceTypes() {
		HashMap<String, String> res = new HashMap<>();
		HashMap<String, List<RowGroupReader>> rowGroupReaders = readerManager.getRowGroupReaderMap();
		for (String deviceUID : rowGroupReaders.keySet()) {

			RowGroupReader rgr = rowGroupReaders.get(deviceUID).get(0);
			res.put(deviceUID, rgr.getDeltaObjectType());
		}
		return res;
	}

	/**
	 * @return res.get(i) represents the End-Position for specific rowGroup i in
	 *         this file.
	 */
	public ArrayList<Long> getRowGroupPosList() {
		ArrayList<Long> res = new ArrayList<>();
		long startPos = 0;
		for (RowGroupReader rowGroupReader : readerManager.getAllRowGroupReaders()) {
			long currentEndPos = rowGroupReader.getTotalByteSize() + startPos;
			res.add(currentEndPos);
			startPos = currentEndPos;
		}
		return res;
	}

	public FilterSeries<?> getColumnBySensorName(String device, String sensor) {
		TSDataType type = readerManager.getDataTypeBySeriesName(device, sensor);
		if (type == TSDataType.INT32) {
			return FilterFactory.intFilterSeries(device, sensor, FilterSeriesType.VALUE_FILTER);
		} else if (type == TSDataType.INT64) {
			return FilterFactory.longFilterSeries(device, sensor, FilterSeriesType.VALUE_FILTER);
		} else if (type == TSDataType.FLOAT) {
			return FilterFactory.floatFilterSeries(device, sensor, FilterSeriesType.VALUE_FILTER);
		} else if (type == TSDataType.DOUBLE) {
			return FilterFactory.doubleFilterSeries(device, sensor, FilterSeriesType.VALUE_FILTER);
		} else if (type == TSDataType.BOOLEAN) {
			return FilterFactory.booleanFilterSeries(device, sensor, FilterSeriesType.VALUE_FILTER);
		} else if (type == TSDataType.BYTE_ARRAY){
			return FilterFactory.stringFilterSeries(device, sensor, FilterSeriesType.VALUE_FILTER);
		} else{
			throw new UnSupportedDataTypeException("Datatype:" + type);
		}
	}

	public ReaderManager getReaderManager() {
		return readerManager;
	}

	/**
	 * {NEWFUNC} use {@code RecordReaderFactory} to manage all RecordReader
	 * 
	 * @throws ProcessorException
	 */
	public void closeFromFactory() throws ProcessorException {
		RecordReaderFactory.getInstance().closeOneRecordReader(this);
	}

	/**
	 * {NEWFUNC} for optimization in recordReaderFactory
	 */
	public void reopenIfChanged() {
		// TODO: how to reopen a recordReader
	}

	/**
	 * {NEWFUNC} close current RecordReader
	 * 
	 * @throws IOException
	 * @throws ProcessorException
	 */
	public void close() throws IOException, ProcessorException {
		readerManager.close();
		// unlock for one subQuery
		ReadLockManager.getInstance().unlockForSubQuery(deltaObjectUID, measurementID, lockToken);
	}
}
