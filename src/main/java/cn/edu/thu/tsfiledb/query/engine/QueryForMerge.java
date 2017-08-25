package cn.edu.thu.tsfiledb.query.engine;

import java.io.IOException;
import java.util.List;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.query.management.ReadLockManager;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;

public class QueryForMerge {

	private List<Path> pathList;
	private SingleSeriesFilterExpression timeFilter;
	private OverflowQueryEngine queryEngine;
	private QueryDataSet queryDataSet;
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();

	public QueryForMerge(List<Path> pathList, SingleSeriesFilterExpression timeFilter) {
		this.pathList = pathList;
		this.timeFilter = timeFilter;
		queryEngine = new OverflowQueryEngine();
		queryDataSet = null;
	}

	public boolean hasNextRecord() {
		boolean ret = false;

		if (queryDataSet == null || !queryDataSet.hasNextRecord()) {
			try {
				queryDataSet = queryEngine.query(0, pathList, timeFilter, null, null, queryDataSet,
						TsFileDBConf.fetchSize);
			} catch (ProcessorException | IOException | PathErrorException e) {
				e.printStackTrace();
			}
		}
		ret = queryDataSet.hasNextRecord();
		if (!ret) {
			unlockForCurrentQuery();
		}
		return ret;
	}

	public RowRecord getNextRecord(){
		if (hasNextRecord()) {
			return queryDataSet.getNextRecord();
		}
		return null;
	}

	private void unlockForCurrentQuery() {
		try {
			ReadLockManager.getInstance().unlockForOneRequest();
		} catch (ProcessorException e) {
			e.printStackTrace();
		}
	}

}
