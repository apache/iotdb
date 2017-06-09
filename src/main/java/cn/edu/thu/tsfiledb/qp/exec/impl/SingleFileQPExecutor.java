package cn.edu.thu.tsfiledb.qp.exec.impl;

import cn.edu.thu.tsfile.common.constant.QueryConstant;
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.query.QueryEngine;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;

import java.io.IOException;
import java.util.List;

public class SingleFileQPExecutor extends QueryProcessExecutor {

	TSRandomAccessFileReader raf;
	QueryEngine queryEngine;

	public SingleFileQPExecutor(TSRandomAccessFileReader raf) throws IOException {
		super(true);
		this.raf = raf;
		queryEngine = new QueryEngine(raf);
	}

	@Override
	protected TSDataType getNonReseveredSeriesType(Path path) {
		return queryEngine.getSeriesType(path);
	}

	@Override
	protected boolean judgeNonReservedPathExists(Path path) {
		return queryEngine.pathExist(path);
	}

	@Override
	public QueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
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
	public boolean update(Path path, long startTime, long endTime, String value) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete(Path path, long deleteTime) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int insert(Path path, long insertTime, String value) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int multiInsert(String deltaObject, long insertTime, List<String> measurementList, List<String> insertValues)
			throws ProcessorException {
		// TODO Auto-generated method stub
		return 0;
	}
}
