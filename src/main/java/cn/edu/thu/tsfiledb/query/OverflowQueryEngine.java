package cn.edu.thu.tsfiledb.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.corp.tsfile.bufferwrite.exception.ProcessorException;
import com.corp.tsfile.exception.metadata.PathErrorException;
import com.corp.tsfile.file.metadata.enums.TSDataType;
import com.corp.tsfile.filter.crossfilter.CrossSensorFilterOperators.CrossSensorFilter;
import com.corp.tsfile.filter.defination.FilterExpression;
import com.corp.tsfile.filter.defination.SingleSensorFilter;
import com.corp.tsfile.filter.utils.FilterUtilsForOverflow;
import com.corp.tsfile.filter.visitorImpl.SingleValueVistor;
import com.corp.tsfile.filter.visitorImpl.VistorFactory;
import com.corp.tsfile.overflow.io.OverflowManager;
import com.corp.tsfile.qp.exec.Path;
import com.corp.tsfile.read.DynamicOneColumnData;
import com.corp.tsfile.read.RecordReader;
import com.corp.tsfile.read.aggregation.AggreFuncFactory;
import com.corp.tsfile.read.aggregation.AggregateFunction;
import com.corp.tsfile.read.aggregation.AggregationResult;
import com.corp.tsfile.read.management.RecordReaderFactory;
import com.corp.tsfile.read.metadata.MManager;
import com.corp.tsfile.read.query.BatchReadRecordGenerator;
import com.corp.tsfile.read.query.CrossQueryTimeGenerator;
import com.corp.tsfile.read.query.QueryConfig;
import com.corp.tsfile.read.query.QueryDataSet;

public class OverflowQueryEngine {
	private static final Logger logger = LoggerFactory.getLogger(OverflowQueryEngine.class);
	private RecordReaderFactory recordReaderFactory;
	private MManager mManager;
	
	public OverflowQueryEngine() {
		recordReaderFactory = RecordReaderFactory.getInstance();
		mManager = MManager.getInstance();
	}
	
	public QueryDataSet query(QueryConfig config, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
		initQueryDataSet(queryDataSet);
		if (config.getQueryType() == QueryConfig.QUERY_WITHOUT_FILTER) {
			return readWithoutFilter(config, queryDataSet, fetchSize);
		} else if (config.getQueryType() == QueryConfig.SELECT_ONE_COL_WITH_FILTER) {
			return readOneColumnValueUseFilter(config, queryDataSet, fetchSize);
		} else if (config.getQueryType() == QueryConfig.CROSS_QUERY) {
			return crossColumnQuery(config, queryDataSet, fetchSize);
		}
		return null;
	}

	public QueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
			FilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
		initQueryDataSet(queryDataSet);
		if (timeFilter == null && freqFilter == null && valueFilter == null) {
			return readWithoutFilter(paths, queryDataSet, fetchSize);
		} else if (valueFilter != null && valueFilter instanceof CrossSensorFilter) {
			return crossColumnQuery(paths, (SingleSensorFilter) timeFilter, (SingleSensorFilter) freqFilter,
					(CrossSensorFilter) valueFilter, queryDataSet, fetchSize);
		} else {
			return readOneColumnValueUseFilter(paths, (SingleSensorFilter) timeFilter, (SingleSensorFilter) freqFilter,
					(SingleSensorFilter) valueFilter, queryDataSet, fetchSize);
		}
	}
	
	public QueryDataSet aggregate(Path path, String aggrFuncName
		, FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws ProcessorException, IOException{
		TSDataType dataType;
		try {
			dataType = MManager.getInstance().getSeriesType(path.getFullPath());
		} catch (PathErrorException e) {
			throw new ProcessorException(e);
		}
		AggregateFunction func = AggreFuncFactory.getAggrFuncByName(aggrFuncName, dataType);
		return aggregate(path, func, timeFilter, freqFilter, valueFilter);
	}
	
	public QueryDataSet aggregate(Path path, AggregateFunction func 
			, FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws ProcessorException, IOException{
		if(!(timeFilter == null || timeFilter instanceof SingleSensorFilter) || 
		   !(freqFilter == null || freqFilter instanceof SingleSensorFilter) ||
		   !(valueFilter == null || valueFilter instanceof SingleSensorFilter)){
			throw new ProcessorException("Filter must be SingleSensorFilter");
		}
		
		QueryDataSet queryDataSet = new QueryDataSet();
		String deltaObjectUID = path.getDeviceToString();
		String measuremetnUID = path.getSensorToString();

		RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measuremetnUID);
		// Get 4 params
		List<Object> params = getOverflowInfoAndFilterDataInMem(deltaObjectUID, measuremetnUID, 
				(SingleSensorFilter)timeFilter, (SingleSensorFilter)freqFilter, (SingleSensorFilter)valueFilter
				, null, recordReader.insertDataInMemory);
		DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
		DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
		DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
		SingleSensorFilter delteFilter = (SingleSensorFilter) params.get(3);
		
		AggregationResult aggrRet = recordReader.aggregate(deltaObjectUID, measuremetnUID, func, 
				updateTrue, updateFalse, insertTrue
				, delteFilter, (SingleSensorFilter)freqFilter, (SingleSensorFilter)valueFilter);
		
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
		
		String device = path.getDeviceToString();
		String sensor = path.getSensorToString();

		RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor);
		
		// Get 4 params
		List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, null, null, null, res, recordReader.insertDataInMemory);
		DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
		DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
		DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
		SingleSensorFilter delteFilter = (SingleSensorFilter) params.get(3);
		
		res = recordReader.getValueInOneColumnWithOverflow(device, sensor,
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
	
	/**
	 * function 1: aimed to getIndex without filter
	 * 
	 * @param recordReader
	 * @param config
	 * @return
	 * @throws ProcessorException 
	 * @throws IOException 
	 */
	public QueryDataSet readWithoutFilter(QueryConfig config, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
		initQueryDataSet(queryDataSet);
		// QueryDataSet ret = queryDataSet;
		for (String s : config.getSelectColumns()) {
			String args[] = s.split(",");
			String device = args[0];
			String sensor = args[1];

			DynamicOneColumnData res = null;
			if (queryDataSet != null) {
				res = queryDataSet.mapRet.get(s);
			} else {
				queryDataSet = new QueryDataSet();
			}

			RecordReader recordReader = recordReaderFactory.getRecordReader(device, sensor);
			
			// Get 4 params
			List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, null, null, null, res, recordReader.insertDataInMemory);
			DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
			DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
			DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
			SingleSensorFilter timeFilter = (SingleSensorFilter) params.get(3);
			
			DynamicOneColumnData oneColDataList = recordReader.getValueInOneColumnWithOverflow(device, sensor,
					updateTrue, updateFalse, insertTrue, timeFilter, res, fetchSize);
			queryDataSet.mapRet.put(s, oneColDataList);
			oneColDataList.putOverflowInfo(insertTrue, updateTrue, updateFalse,timeFilter);
			
			recordReader.closeFromFactory();
		}
		return queryDataSet;
	}

	/**
	 * Temporary function for QP 2
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
//	public static QueryDataSet readOneColumnValueUseFilter(List<Path> paths, SingleSensorFilter timeFilter,
//			SingleSensorFilter freqFilter, SingleSensorFilter valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException {
//		logger.info("start read one column data with filter...");
//		initQueryDataSet(queryDataSet);
//		// QueryDataSet ret = new QueryDataSet();
//		// only one devSen allowed
//		for (Path p : paths) {
//			String device = p.getDeviceToString();
//			String sensor = p.getSensorToString();
//			String devSen = device + "." + sensor;
//
//			DynamicOneColumnData res = null;
//			if (queryDataSet != null) {
//				res = queryDataSet.mapRet.get(devSen);
//			} else {
//				queryDataSet = new QueryDataSet();
//			}
//			
//			RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor);
//			// Get 4 params
//			List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, timeFilter, freqFilter, valueFilter, res, recordReader.insertDataInMemory);
//			DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
//			DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
//			DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
//			SingleSensorFilter deleteFilter = (SingleSensorFilter) params.get(3);
//
//			DynamicOneColumnData oneColDataList = recordReader.getValueWithFilterAndOverflow(device, sensor, updateTrue,
//					updateFalse, insertTrue, deleteFilter, freqFilter, valueFilter, res, fetchSize);
//			queryDataSet.mapRet.put(devSen, oneColDataList);
//			oneColDataList.putOverflowInfo(insertTrue, updateTrue, updateFalse,deleteFilter);
//			
//			recordReader.closeFromFactory();
//		}
//
//		return queryDataSet;
//	}

	public static QueryDataSet readOneColumnValueUseFilter(List<Path> paths, SingleSensorFilter timeFilter,
			SingleSensorFilter freqFilter, SingleSensorFilter valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
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
	
	public static DynamicOneColumnData readOneColumnValueUseFilter(Path p, SingleSensorFilter timeFilter,
			SingleSensorFilter freqFilter, SingleSensorFilter valueFilter, DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException{
		String device = p.getDeviceToString();
		String sensor = p.getSensorToString();
		
		RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor);
		// Get 4 params
		List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, timeFilter, freqFilter, valueFilter, res, recordReader.insertDataInMemory);
		DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
		DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
		DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
		SingleSensorFilter deleteFilter = (SingleSensorFilter) params.get(3);

		DynamicOneColumnData oneColDataList = recordReader.getValueWithFilterAndOverflow(device, sensor, updateTrue,
				updateFalse, insertTrue, deleteFilter, freqFilter, valueFilter, res, fetchSize);
		oneColDataList.putOverflowInfo(insertTrue, updateTrue, updateFalse,deleteFilter);
		recordReader.closeFromFactory();
		return oneColDataList;
	}
	
	/**
	 * QueryFunciton 2: aimed to getIndex with filter, BUT only one column allowed
	 * @throws ProcessorException 
	 * @throws IOException 
	 */
	public static QueryDataSet readOneColumnValueUseFilter(QueryConfig config, QueryDataSet queryDataSet,
			int fetchSize) throws ProcessorException, IOException {
		logger.info("start read one column data with filter...");
		initQueryDataSet(queryDataSet);
		
		SingleSensorFilter timeFilter = FilterUtilsForOverflow.construct(config.getTimeFilter());
		SingleSensorFilter freqFilter = FilterUtilsForOverflow.construct(config.getFreqFilter());
		SingleSensorFilter valueFilter = FilterUtilsForOverflow.construct(config.getValueFilter());

		// QueryDataSet ret = new QueryDataSet();
		// only one devSen allowed
		for (int i = 0; i < config.getSelectColumns().size(); i++) {
			String devSen = config.getSelectColumns().get(i);
			String[] args = devSen.split(",");
			String device = args[0];
			String sensor = args[1];

			DynamicOneColumnData res = null;
			if (queryDataSet != null) {
				res = queryDataSet.mapRet.get(devSen);
			} else {
				queryDataSet = new QueryDataSet();
			}

			RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor);
			// Get 4 params
			List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, timeFilter, freqFilter, valueFilter, res, recordReader.insertDataInMemory);
			DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
			DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
			DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
			SingleSensorFilter deleteFilter = (SingleSensorFilter) params.get(3);
			
			DynamicOneColumnData oneColDataList = recordReader.getValueWithFilterAndOverflow(device, sensor, updateTrue,
					updateFalse, insertTrue, deleteFilter, freqFilter, valueFilter, res, fetchSize);
			queryDataSet.mapRet.put(devSen, oneColDataList);
			oneColDataList.putOverflowInfo(insertTrue, updateTrue, updateFalse,deleteFilter);
			
			recordReader.closeFromFactory();
		}

		return queryDataSet;
	}

	// Method for CrossQueryTimeGenerator.
	public static DynamicOneColumnData readOneColumnValueUseValueFilter(SingleSensorFilter valueFilter,
			SingleSensorFilter freqFilter, DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException {
		
		String device = ((SingleSensorFilter) valueFilter).getColumn().getDeviceName();
		String sensor = ((SingleSensorFilter) valueFilter).getColumn().getSensorName();
		
		RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor);
		// Get 4 params
		List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, null, freqFilter, valueFilter, res, recordReader.insertDataInMemory);
		DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
		DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
		DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
		SingleSensorFilter deleteFilter = (SingleSensorFilter) params.get(3);
		
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
	public static QueryDataSet crossColumnQuery(List<Path> paths, SingleSensorFilter timeFilter,
			SingleSensorFilter freqFilter, CrossSensorFilter valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
		logger.info("start cross columns getIndex...");
		initQueryDataSet(queryDataSet);
		// Step 1: calculate common timestamp
		logger.info("step 1: init time value generator...");
		if (queryDataSet == null) {
			queryDataSet = new QueryDataSet();
			queryDataSet.timeQueryDataSet = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, fetchSize){
				@Override
				public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
						SingleSensorFilter valueFilter) throws ProcessorException, IOException {
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

			String device = p.getDeviceToString();
			String sensor = p.getSensorToString();
			String s = device + "." + sensor;
			
			RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor);
			
			// Get 4 params
			List<Object> params = getOverflowInfoAndFilterDataInMem(device, sensor, null, null, null, null, recordReader.insertDataInMemory);
			DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
			DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
			SingleSensorFilter deleteFilter = (SingleSensorFilter) params.get(3);
			
			DynamicOneColumnData oneColDataList = recordReader.getValuesUseTimeValueWithOverflow(device, sensor,
					timeRet, updateTrue, insertTrue, deleteFilter);
			ret.mapRet.put(s, oneColDataList);
			
			recordReader.closeFromFactory();
		}
		return ret;
	}

	/**
	 * QueryFunciton 3: Function for Cross Columns Query
	 * 
	 * @param recordReader
	 * @param config
	 * @return
	 * @throws ProcessorException 
	 * @throws IOException 
	 */
	public static QueryDataSet crossColumnQuery(QueryConfig config, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
		logger.info("start cross columns getIndex...");
		initQueryDataSet(queryDataSet);
		SingleSensorFilter timeFilter = FilterUtilsForOverflow.construct(config.getTimeFilter());
		SingleSensorFilter freqFilter = FilterUtilsForOverflow.construct(config.getFreqFilter());
		CrossSensorFilter valueFilter = (CrossSensorFilter) FilterUtilsForOverflow
				.constructCrossFilter(config.getValueFilter());
		List<Path> paths = new ArrayList<Path>();
		for(String s : config.getSelectColumns()){
			Path p = new Path(s);
			paths.add(p);
		}
		return crossColumnQuery(paths, timeFilter, freqFilter, valueFilter, queryDataSet, fetchSize);
	}
	
	public static List<Object> getOverflowInfoAndFilterDataInMem(String deltaObjectUID,String measurementID,
			SingleSensorFilter timeFilter, SingleSensorFilter freqFilter,SingleSensorFilter valueFilter
			, DynamicOneColumnData res, DynamicOneColumnData insertDataInMemory) throws ProcessorException{
				
		List<Object> paramList = new ArrayList<Object>();
		
		if (res == null) {
			//Filter satisfied value from insertDataInMemory
			OverflowManager processor = OverflowManager.getInstance();
			List<Object> params = processor.query(deltaObjectUID, measurementID, timeFilter, freqFilter, valueFilter);
			timeFilter = (SingleSensorFilter) params.get(3);
			
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
	public static DynamicOneColumnData getSatisfiedData(DynamicOneColumnData updateTrue, SingleSensorFilter timeFilter, SingleSensorFilter freqFilter
			,SingleSensorFilter valueFilter, DynamicOneColumnData oneColData){
		if(oneColData == null){
			return null;
		}
		if(oneColData.length == 0){
		    return oneColData;
		}
		
		//update the value in oneColData according to updateTrue
		oneColData = updateValueAccordingToUpdateTrue(updateTrue, oneColData);
		DynamicOneColumnData res = new DynamicOneColumnData(oneColData.dataType, true);
		SingleValueVistor<?> singleValueVisitor = VistorFactory.getSingleValueVistor(oneColData.dataType);
		switch(oneColData.dataType){
		case BOOLEAN:
			for(int i = 0 ; i < oneColData.length ; i++){
				boolean v = oneColData.getBoolean(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && singleValueVisitor.satisfyObject(v, valueFilter)) ||
						(valueFilter == null && timeFilter != null && singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter)) ||
						(valueFilter != null && timeFilter != null && 
						singleValueVisitor.satisfyObject(v, valueFilter)  && 
						singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter))) {
					res.putBoolean(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		case DOUBLE:
			for(int i = 0 ; i < oneColData.length ; i++){
				double v = oneColData.getDouble(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && singleValueVisitor.satisfyObject(v, valueFilter)) ||
						(valueFilter == null && timeFilter != null && singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter)) ||
						(valueFilter != null && timeFilter != null && 
						singleValueVisitor.satisfyObject(v, valueFilter)  && 
						singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter))) {
					res.putDouble(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		case FLOAT:
			for(int i = 0 ; i < oneColData.length ; i++){
				float v = oneColData.getFloat(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && singleValueVisitor.satisfyObject(v, valueFilter)) ||
						(valueFilter == null && timeFilter != null && singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter)) ||
						(valueFilter != null && timeFilter != null && 
						singleValueVisitor.satisfyObject(v, valueFilter)  && 
						singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter))) {
					res.putFloat(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		case INT32:
			for(int i = 0 ; i < oneColData.length ; i++){
				int v = oneColData.getInt(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && singleValueVisitor.satisfyObject(v, valueFilter)) ||
						(valueFilter == null && timeFilter != null && singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter)) ||
						(valueFilter != null && timeFilter != null && 
						singleValueVisitor.satisfyObject(v, valueFilter)  && 
						singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter))) {
					res.putInt(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		case INT64:
			for(int i = 0 ; i < oneColData.length ; i++){
				long v = oneColData.getLong(i);
				if ((valueFilter == null && timeFilter == null) || 
						(valueFilter != null && timeFilter == null && singleValueVisitor.satisfyObject(v, valueFilter)) ||
						(valueFilter == null && timeFilter != null && singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter)) ||
						(valueFilter != null && timeFilter != null && 
						singleValueVisitor.satisfyObject(v, valueFilter)  && 
						singleValueVisitor.satisfyObject(oneColData.getTime(i), timeFilter))) {
					res.putLong(v);
					res.putTime(oneColData.getTime(i));
				}
			}
			break;
		default:
			break;
		}
		
		return res;
	}
}















