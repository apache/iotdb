package cn.edu.thu.tsfiledb.qp.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.query.engine.FilterStructure;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryEngine;

public class SingleFileQPExecutor extends QueryProcessExecutor {

	private QueryEngine queryEngine;

	public SingleFileQPExecutor(TSRandomAccessFileReader raf) throws IOException {
		queryEngine = new QueryEngine(raf);
	}

	@Override
	public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException{
		throw new ProcessorException("Do not support");
	}

	@Override
	protected TSDataType getNonReservedSeriesType(Path path) {
		return queryEngine.getSeriesType(path);
	}

	@Override
	protected boolean judgeNonReservedPathExists(Path path) {
		return queryEngine.pathExist(path);
	}

	@Override
	public QueryDataSet aggregate(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures)
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
	public boolean delete(Path path, long deleteTime) throws ProcessorException{
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
