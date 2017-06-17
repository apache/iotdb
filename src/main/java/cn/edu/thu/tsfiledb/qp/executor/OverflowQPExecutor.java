package cn.edu.thu.tsfiledb.qp.executor;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;

public class OverflowQPExecutor extends QueryProcessExecutor {

	private static final Logger logger = LoggerFactory.getLogger(OverflowQPExecutor.class);
	private OverflowQueryEngine queryEngine;
	private FileNodeManager fileNodeManager;

	public OverflowQPExecutor() {
		queryEngine = new OverflowQueryEngine();
		fileNodeManager = FileNodeManager.getInstance();
	}

	@Override
	protected TSDataType getNonReservedSeriesType(Path path) throws PathErrorException {
		return MManager.getInstance().getSeriesType(path.getFullPath());
	}

	@Override
	protected boolean judgeNonReservedPathExists(Path path) {
		return MManager.getInstance().pathExist(path.getFullPath());
	}

	@Override
	public QueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
			FilterExpression valueFilter, int fetchSize, QueryDataSet lastData) throws ProcessorException {
		QueryDataSet ret;
		try {
			if (parameters.get() != null && parameters.get().containsKey(SQLConstant.IS_AGGREGATION)) {
				if (lastData != null) {
					lastData.clear();
					return lastData;
				}
				String aggregateFuncName = (String) parameters.get().get(SQLConstant.IS_AGGREGATION);
				ret = queryEngine.aggregate(paths.get(0), aggregateFuncName, timeFilter, freqFilter, valueFilter);
			} else {
				ret = queryEngine.query(paths, timeFilter, freqFilter, valueFilter, lastData, fetchSize);
			}

			return ret;
		} catch (Exception e) {
			logger.error("Error in query", e);
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public boolean update(Path path, long startTime, long endTime, String value) throws ProcessorException {
		String device = path.getDeltaObjectToString();
		String sensor = path.getMeasurementToString();

		try {
			TSDataType type = queryEngine.getDataTypeByDeviceAndSensor(device, sensor);
			fileNodeManager.update(device, sensor, startTime, endTime, type, value);
			return true;
		} catch (PathErrorException e) {
			throw new ProcessorException("Error in update: " + e.getMessage());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			throw new ProcessorException(e);
		}
	}

	@Override
	public boolean delete(Path path, long timestamp) throws ProcessorException {
		String device = path.getDeltaObjectToString();
		String sensor = path.getMeasurementToString();
		try {
			TSDataType type = queryEngine.getDataTypeByDeviceAndSensor(device, sensor);
			fileNodeManager.delete(device, sensor, timestamp, type);
			return true;
		} catch (PathErrorException e) {
			throw new ProcessorException("Error in delete: " + e.getMessage());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			throw new ProcessorException(e);
		}
	}

	@Override
	// return 0: failed, 1: Overflow, 2:Bufferwrite
	public int insert(Path path, long timestamp, String value) throws ProcessorException {
		String device = path.getDeltaObjectToString();
		String sensor = path.getMeasurementToString();

		try {
			TSDataType type = queryEngine.getDataTypeByDeviceAndSensor(device, sensor);
			TSRecord tsRecord = new TSRecord(timestamp, device);
			DataPoint dataPoint = DataPoint.getDataPoint(type, sensor, value);
			tsRecord.addTuple(dataPoint);
			return fileNodeManager.insert(tsRecord);

		} catch (PathErrorException e) {
			throw new ProcessorException("Error in insert: " + e.getMessage());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			throw new ProcessorException(e);
		}
	}

	@Override
	public int multiInsert(String deltaObject, long insertTime, List<String> measurementList, List<String> insertValues)
			throws ProcessorException {
		try {
			MManager mManager = MManager.getInstance();
			TSRecord tsRecord = new TSRecord(insertTime, deltaObject);
			
			for (int i = 0 ; i < measurementList.size() ; i ++) {
				String p = deltaObject + "." + measurementList.get(i);
				if(!mManager.pathExist(p)){
					throw new ProcessorException("Path not exists:" + p);
				}
				TSDataType dataType = mManager.getSeriesType(p);
				DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementList.get(i), insertValues.get(i));
				tsRecord.addTuple(dataPoint);
			}
			return fileNodeManager.insert(tsRecord);
			
		} catch (PathErrorException e) {
			throw new ProcessorException("Path error:" + e.getMessage());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			throw new ProcessorException(e);
		}
	}

}















