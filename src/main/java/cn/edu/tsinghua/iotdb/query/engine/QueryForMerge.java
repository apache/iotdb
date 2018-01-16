package cn.edu.tsinghua.iotdb.query.engine;

import java.io.IOException;
import java.util.List;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.management.ReadLockManager;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to query the data in file merging between tsfile and overflow data.
 */
public class QueryForMerge {

	private static final Logger LOGGER = LoggerFactory.getLogger(QueryForMerge.class);

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
						TsFileDBConf.fetchSize, null);
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
		} catch (ProcessorException | IOException e) {
			LOGGER.error("meet error in jdbc close operation, because of {}", e.getMessage());
			e.printStackTrace();
		}
	}

}
