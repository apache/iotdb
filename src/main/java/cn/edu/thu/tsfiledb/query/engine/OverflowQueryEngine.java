package cn.edu.thu.tsfiledb.query.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.BatchReadRecordGenerator;
import cn.edu.thu.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.aggregation.AggreFuncFactory;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.aggregation.AggregationResult;
import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;
import cn.edu.thu.tsfiledb.query.reader.RecordReader;


public class OverflowQueryEngine {
	private static final Logger logger = LoggerFactory.getLogger(OverflowQueryEngine.class);
	private RecordReaderFactory recordReaderFactory;
	private MManager mManager;
	
	public OverflowQueryEngine() {
		recordReaderFactory = RecordReaderFactory.getInstance();
		mManager = MManager.getInstance();
	}

	public QueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
			FilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
		initQueryDataSet(queryDataSet);
		if (timeFilter == null && freqFilter == null && valueFilter == null) {
			return readWithoutFilter(paths, queryDataSet, fetchSize);
		} else if (valueFilter != null && valueFilter instanceof CrossSeriesFilterExpression) {
			return crossColumnQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
					(CrossSeriesFilterExpression) valueFilter, queryDataSet, fetchSize);
		} else {
			return readOneColumnValueUseFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
					(SingleSeriesFilterExpression) valueFilter, queryDataSet, fetchSize);
		}
	}
	
	public QueryDataSet aggregate(Path path, String aggrFuncName
		, FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws ProcessorException, IOException{
		TSDataType dataType;
		try {
			dataType = MManager.getInstance().getSeriesType(path.getFullPath());
		} catch (PathErrorException e) {
			throw new ProcessorException(e.getMessage());
		}
		AggregateFunction func = AggreFuncFactory.getAggrFuncByName(aggrFuncName, dataType);
		return aggregate(path, func, timeFilter, freqFilter, valueFilter);
	}
	
	public QueryDataSet aggregate(Path path, AggregateFunction func 
			, FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws ProcessorException, IOException{
		if(!(timeFilter == null || timeFilter instanceof SingleSeriesFilterExpression) || 
		   !(freqFilter == null || freqFilter instanceof SingleSeriesFilterExpression) ||
		   !(valueFilter == null || valueFilter instanceof SingleSeriesFilterExpression)){
			throw new ProcessorException("Filter must be SingleSeriesFilterExpression");
		}
		
		QueryDataSet queryDataSet = new QueryDataSet();
		String deltaObjectUID = path.getDeltaObjectToString();
		String measuremetnUID = path.getMeasurementToString();

		RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measuremetnUID,
				(SingleSeriesFilterExpression)timeFilter, 
				(SingleSeriesFilterExpression)freqFilter, 
				(SingleSeriesFilterExpression)valueFilter);
		// Get 4 params
		List<Object> params = getOverflowInfoAndFilterDataInMem(deltaObjectUID, measuremetnUID, 
				(SingleSeriesFilterExpression)timeFilter, (SingleSeriesFilterExpression)freqFilter, (SingleSeriesFilterExpression)valueFilter
				, null, recordReader.insertDataInMemory, recordReader.overflowInfo);
		DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
		DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
		DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
		SingleSeriesFilterExpression delteFilter = (SingleSeriesFilterExpression) params.get(3);
		
		AggregationResult aggrRet = recordReader.aggregate(deltaObjectUID, measuremetnUID, func, 
				updateTrue, updateFalse, insertTrue
				, delteFilter, (SingleSeriesFilterExpression)freqFilter, (SingleSeriesFilterExpression)valueFilter);
		
		queryDataSet.mapRet.put(func.name + "(" + path.getFullPath() + ")", aggrRet.data);
		//close current recordReader
		recordReader.closeFromFactory();
		
		return queryDataSet;
		
	}
	
	public QueryDataSet readWithoutFilter(List<Path> paths, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
		if(queryDataSet == null){
			queryDataSet = new QueryDataSet();
			BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize){
				@Override
				public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
					return OverflowQueryEngine.readOneColumnWithoutFilter(p, res, fetchSize);
				}
			
			};
			queryDataSet.setBatchReaderRetGenerator(batchReaderRetGenerator);
		}
		initQueryDataSet(queryDataSet);
		queryDataSet.getBatchReaderRetGenerator().calculateRecord();
		queryDataSet.putRecordFromBatchReadRetGenerator();
		return queryDataSet;
	}
	
	public static DynamicOneColumnData readOneColumnWithoutFilter(Path path, DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException{
		
		String deltaObjectUID = path.getDeltaObjectToString();
		String measuremetnUID = path.getMeasurementToString();

		RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measuremetnUID,null, null, null);
		
		// Get 4 params
		List<Object> params = getOverflowInfoAndFilterDataInMem(deltaObjectUID, measuremetnUID, null, null, null, res, recordReader.insertDataInMemory, recordReader.overflowInfo);
		DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
		DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
		DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
		SingleSeriesFilterExpression delteFilter = (SingleSeriesFilterExpression) params.get(3);
		
		res = recordReader.getValueInOneColumnWithOverflow(deltaObjectUID, measuremetnUID,
				updateTrue, updateFalse, insertTrue, delteFilter, res, fetchSize);
		
		res.putOverflowInfo(insertTrue, updateTrue, updateFalse,delteFilter);
		//close current recordReader
		recordReader.closeFromFactory();
		
		return res;
	}
	
	public TSDataType getDataTypeByDeviceAndSensor(String device, String sensor) throws PathErrorException {
		String path = device + "." + sensor;
		return mManager.getSeriesType(path);
	}

	public static void initQueryDataSet(QueryDataSet queryDataSet){
		if(queryDataSet != null){
			queryDataSet.clear();
		}
	}
	

	public static QueryDataSet readOneColumnValueUseFilter(List<Path> paths, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
		if(queryDataSet == null){
			queryDataSet = new QueryDataSet();
			BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize){
				@Override
				public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
					return OverflowQueryEngine.readOneColumnValueUseFilter(p, timeFilter, freqFilter, valueFilter, res, fetchSize);
				}
			};
			queryDataSet.setBatchReaderRetGenerator(batchReaderRetGenerator);
		}
		initQueryDataSet(queryDataSet);
		queryDataSet.getBatchReaderRetGenerator().calculateRecord();
		queryDataSet.putRecordFromBatchReadRetGenerator();
		return queryDataSet;
	}
	
	public static DynamicOneColumnData readOneColumnValueUseFilter(Path p, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException{
		String device = p.getDeltaObjectToString();
		String sensor = p.getMeasurementToString();
		
		RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor, timeFilter, freqFilter, valueFilter);
		// Get 4 params
		List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, timeFilter, freqFilter, valueFilter, res, recordReader.insertDataInMemory, recordReader.overflowInfo);
		DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
		DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
		DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
		SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) params.get(3);

		DynamicOneColumnData oneColDataList = recordReader.getValueWithFilterAndOverflow(device, sensor, updateTrue,
				updateFalse, insertTrue, deleteFilter, freqFilter, valueFilter, res, fetchSize);
		oneColDataList.putOverflowInfo(insertTrue, updateTrue, updateFalse,deleteFilter);
		recordReader.closeFromFactory();
		return oneColDataList;
	}

	// Method for CrossQueryTimeGenerator.
	public static DynamicOneColumnData readOneColumnValueUseValueFilter(SingleSeriesFilterExpression valueFilter,
			SingleSeriesFilterExpression freqFilter, DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException {
		
		String device = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getDeltaObjectUID();
		String sensor = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getMeasurementUID();
		
		RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor, null, freqFilter, valueFilter);
		// Get 4 params
		List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, null, freqFilter, valueFilter, res, recordReader.insertDataInMemory, recordReader.overflowInfo);
		DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
		DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
		DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
		SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) params.get(3);
		
		res = recordReader.getValueWithFilterAndOverflow(device, sensor, updateTrue, updateFalse, insertTrue,
				deleteFilter, freqFilter, valueFilter, res, fetchSize);
		res.putOverflowInfo(insertTrue, updateTrue, updateFalse,deleteFilter);
		
		recordReader.closeFromFactory();
		return res;
	}

	/**
	 * Temporary function for QP 3
	 * 
	 * @param paths
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @param queryDataSet
	 * @param fetchSize
	 * @return
	 * @throws ProcessorException 
	 * @throws IOException 
	 */
	public static QueryDataSet crossColumnQuery(List<Path> paths, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, CrossSeriesFilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
		logger.info("start cross columns getIndex...");
		initQueryDataSet(queryDataSet);
		// Step 1: calculate common timestamp
		logger.info("step 1: init time value generator...");
		if (queryDataSet == null) {
			queryDataSet = new QueryDataSet();
			queryDataSet.timeQueryDataSet = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, fetchSize){
				@Override
				public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
						SingleSeriesFilterExpression valueFilter) throws ProcessorException, IOException {
					return OverflowQueryEngine.readOneColumnValueUseValueFilter(valueFilter, freqFilter, res, fetchSize);
				}
			};
		}
		logger.info("step 1 done.");
		logger.info("step 2: calculate timeRet...");
		long[] timeRet = queryDataSet.timeQueryDataSet.generateTimes();
		logger.info("step 2 done. timeRet size is: " + timeRet.length + ", FetchSize is: " + fetchSize);

		// Step 3: Get result using common timestamp
		logger.info("step 3: Get result using common timestamp");

		QueryDataSet ret = queryDataSet;
		for (Path p : paths) {

			String device = p.getDeltaObjectToString();
			String sensor = p.getMeasurementToString();
			String s = device + "." + sensor;
			
			RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor, null, null, null);
			
			// Get 4 params
			List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, null, null, null, null, recordReader.insertDataInMemory, recordReader.overflowInfo);
			DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
			DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
			SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) params.get(3);
			
			DynamicOneColumnData oneColDataList = recordReader.getValuesUseTimeValueWithOverflow(device, sensor,
					timeRet, updateTrue, insertTrue, deleteFilter);
			ret.mapRet.put(s, oneColDataList);
			
			recordReader.closeFromFactory();
		}
		return ret;
	}
	
	public static List<Object> getOverflowInfoAndFilterDataInMem(String deltaObjectUID,String measurementID,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,SingleSeriesFilterExpression valueFilter
			, DynamicOneColumnData res, DynamicOneColumnData insertDataInMemory, List<Object> params) throws ProcessorException{
				
		List<Object> paramList = new ArrayList<Object>();
		
		if (res == null) {
			//Filter satisfied value from insertDataInMemory
			timeFilter = (SingleSeriesFilterExpression) params.get(3);
			
			DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
			insertDataInMemory = getSatisfiedData(updateTrue, timeFilter, freqFilter, valueFilter, insertDataInMemory);
			
			DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
			//add insert records from memory in BufferWriter stage
			if(insertTrue == null){
				insertTrue = insertDataInMemory;
			}else{
				insertTrue = mergeInsertTrueAndInsertDataInMemory(insertTrue, insertDataInMemory);
			}
			paramList.add(insertTrue);
			paramList.add(params.get(1));
			paramList.add(params.get(2));
			paramList.add(params.get(3));
		} else {
			paramList.add(res.insertTrue);
			paramList.add(res.updateTrue);
			paramList.add(res.updateFalse);
			paramList.add(res.timeFilter);
		}
		
		return paramList;
	}
	
	/**
	 * Merge insert data in overflow and memory.<br> 
	 * Important: If there is two fields whose timestamp are equal, use the value 
	 * from overflow.
	 * @param insertTrue
	 * @param insertDataInMemory
	 * @return
	 */
	public static DynamicOneColumnData mergeInsertTrueAndInsertDataInMemory(
			DynamicOneColumnData insertTrue, DynamicOneColumnData insertDataInMemory){
		if(insertTrue == null && insertDataInMemory == null){
			return null;
		}
		if(insertTrue != null && insertDataInMemory == null){
			return insertTrue;
		}
		if(insertTrue == null && insertDataInMemory != null){
			return insertDataInMemory;
		}
		
		DynamicOneColumnData res = new DynamicOneColumnData(insertTrue.dataType, true);
		int idx1 = 0;
		int idx2 = 0;
		while(idx1 < insertTrue.length || idx2 < insertDataInMemory.length ){
			while(idx1 < insertTrue.length && (idx2 >= insertDataInMemory.length || 
					insertDataInMemory.getTime(idx2) >= insertTrue.getTime(idx1))){
				res.putTime(insertTrue.getTime(idx1));
				res.putAValueFromDynamicOneColumnData(insertTrue, idx1);
				if(idx2 < insertDataInMemory.length && insertDataInMemory.getTime(idx2) == insertTrue.getTime(idx1)){
					idx2 ++;
				}
				idx1 ++;
			}
			
			while(idx2 < insertDataInMemory.length && (idx1 >= insertTrue.length || 
					insertTrue.getTime(idx1) > insertDataInMemory.getTime(idx2))){
				res.putTime(insertDataInMemory.getTime(idx2));
				res.putAValueFromDynamicOneColumnData(insertDataInMemory, idx2);
				idx2 ++;
			}
		}
		
		return res;
	}
	
	public static DynamicOneColumnData updateValueAccordingToUpdateTrue(DynamicOneColumnData updateTrue
			, DynamicOneColumnData oneColData){
		if(updateTrue == null){
			return oneColData;
		}
		if(oneColData == null){
			return null;
		}
		int idx = 0;
		for(int i = 0 ; i < updateTrue.length ; i++){
			while(idx < oneColData.length && updateTrue.getTime(i*2 + 1) >= oneColData.getTime(idx)){
				if(updateTrue.getTime(i) <= oneColData.getTime(idx)){
					oneColData.updateAValueFromDynamicOneColumnData(updateTrue, i, idx);
				}
				idx ++;
			}
		}
		
		return oneColData;
	}
	
	/**
	 * Get satisfied values from a DynamicOneColumnData
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @param oneColData
	 * @return
	 */
	public static DynamicOneColumnData getSatisfiedData(DynamicOneColumnData updateTrue, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter
			,SingleSeriesFilterExpression valueFilter, DynamicOneColumnData oneColData){
		if(oneColData == null){
			return null;
		}
		if(oneColData.length == 0){
		    return oneColData;
		}
		
		//update the value in oneColData according to updateTrue
		oneColData = updateValueAccordingToUpdateTrue(updateTrue, oneColData);
		DynamicOneColumnData res = new DynamicOneColumnData(oneColData.dataType, true);
		SingleValueVisitor<?> timeVisitor = null;
		if(timeFilter != null){
			timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
		}
		SingleValueVisitor<?> valueVisitor = null;
		if(valueFilter != null){
			valueVisitor = getSingleValueVisitorByDataType(oneColData.dataType, valueFilter);
		}
		
		switch(oneColData.dataType){
		case BOOLEAN:
			for(int i = 0 ; i < oneColData.length ; i++){
				boolean v = oneColData.getBoolean(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && valueVisitor.satisfyObject(v, valueFilter)) ||
						(valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
						(valueFilter != null && timeFilter != null && 
						valueVisitor.satisfyObject(v, valueFilter)  && 
						timeVisitor.verify(oneColData.getTime(i)))) {
					res.putBoolean(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		case DOUBLE:
			for(int i = 0 ; i < oneColData.length ; i++){
				double v = oneColData.getDouble(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && valueVisitor.verify(v)) ||
						(valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
						(valueFilter != null && timeFilter != null && 
						valueVisitor.verify(v)  && 
						timeVisitor.verify(oneColData.getTime(i)))) {
					res.putDouble(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		case FLOAT:
			for(int i = 0 ; i < oneColData.length ; i++){
				float v = oneColData.getFloat(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && valueVisitor.verify(v)) ||
						(valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
						(valueFilter != null && timeFilter != null && 
						valueVisitor.verify(v)  && 
						timeVisitor.verify(oneColData.getTime(i)))) {
					res.putFloat(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		case INT32:
			for(int i = 0 ; i < oneColData.length ; i++){
				int v = oneColData.getInt(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && valueVisitor.verify(v)) ||
						(valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
						(valueFilter != null && timeFilter != null && 
						valueVisitor.verify(v)  && 
						timeVisitor.verify(oneColData.getTime(i)))) {
					res.putInt(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		case INT64:
			for(int i = 0 ; i < oneColData.length ; i++){
				long v = oneColData.getLong(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && valueVisitor.verify(v)) ||
						(valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
						(valueFilter != null && timeFilter != null && 
						valueVisitor.verify(v)  && 
						timeVisitor.verify(oneColData.getTime(i)))) {
					res.putLong(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		case BYTE_ARRAY:
			for(int i = 0 ; i < oneColData.length ; i++){
				Binary v = oneColData.getBinary(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && valueVisitor.satisfyObject(v, valueFilter)) ||
						(valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
						(valueFilter != null && timeFilter != null && 
						valueVisitor.satisfyObject(v, valueFilter)  && 
						timeVisitor.verify(oneColData.getTime(i)))) {
					res.putBinary(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		default:
			break;
		}
		
		return res;
	}
	
	protected static SingleValueVisitor<?> getSingleValueVisitorByDataType(TSDataType type, SingleSeriesFilterExpression filter) {
		switch (type) {
		case INT32:
			return new SingleValueVisitor<Integer>(filter);
		case INT64:
			return new SingleValueVisitor<Long>(filter);
		case FLOAT:
			return new SingleValueVisitor<Float>(filter);
		case DOUBLE:
			return new SingleValueVisitor<Double>(filter);
		default:
			return SingleValueVisitorFactory.getSingleValueVistor(type);
		}
	}
}















