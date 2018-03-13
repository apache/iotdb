package cn.edu.tsinghua.iotdb.qp.executor;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.query.management.FilterStructure;
import cn.edu.tsinghua.iotdb.query.fill.IFill;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryEngine;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SingleFileQPExecutor extends QueryProcessExecutor {

	private QueryEngine queryEngine;

	public SingleFileQPExecutor(ITsRandomAccessFileReader raf) throws IOException {
		queryEngine = new QueryEngine(raf);
	}

	@Override
	public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException{
		throw new ProcessorException("Do not support");
	}

	@Override
	public TSDataType getSeriesType(Path path) {
		// this method is useless
		if (path.equals(SQLConstant.RESERVED_TIME))
			return TSDataType.INT64;
		if (path.equals(SQLConstant.RESERVED_FREQ))
			return TSDataType.FLOAT;
		return TSDataType.INT32;
		//return queryEngine.getSeriesType(path);
	}

	@Override
	public boolean judgePathExists(Path path) {
		if (SQLConstant.isReservedPath(path))
			return true;
		try {
			return queryEngine.pathExist(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public QueryDataSet aggregate(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures)
			throws ProcessorException, IOException, PathErrorException {
		throw new ProcessorException("Do not support");
	}

	@Override
	public QueryDataSet groupBy(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures,
								long unit, long origin, List<Pair<Long, Long>> intervals, int fetchSize)
			throws ProcessorException, IOException, PathErrorException {
		throw new ProcessorException("Do not support");
	}

	@Override
	public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType)
			throws ProcessorException, IOException, PathErrorException {
		throw new ProcessorException("Do not support");
	}

	@Override
	public QueryDataSet query(int formNumber, List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
							  FilterExpression valueFilter, int fetchSize, QueryDataSet lastData) {
		if (lastData != null) {
			lastData.clear();
			return lastData;
		}
		try {
			return queryEngine.query(paths, timeFilter, freqFilter, valueFilter);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public boolean update(Path path, long startTime, long endTime, String value) throws ProcessorException{
		throw new ProcessorException("Do not support");
	}

	@Override
	protected boolean delete(Path path, long deleteTime) throws ProcessorException{
		throw new ProcessorException("Do not support");
	}

	@Override
	public int insert(Path path, long insertTime, String value) throws ProcessorException{
		throw new ProcessorException("Do not support");
	}

	@Override
	public int multiInsert(String deltaObject, long insertTime, List<String> measurementList, List<String> insertValues)
			throws ProcessorException {
		throw new ProcessorException("Do not support");
	}

	@Override
	public List<String> getAllPaths(String originPath) throws PathErrorException {
		List<String> allPaths = new ArrayList<>();
		allPaths.add(originPath);
		return allPaths;
	}
}
