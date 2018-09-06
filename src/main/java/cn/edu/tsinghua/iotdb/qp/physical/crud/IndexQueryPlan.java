package cn.edu.tsinghua.iotdb.qp.physical.crud;


import cn.edu.tsinghua.iotdb.index.IndexManager.IndexType;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class IndexQueryPlan extends PhysicalPlan {

	public IndexType getIndexType() {
		return indexType;
	}

	protected final IndexType indexType;
	protected List<Path> paths;
	protected long startTime;
	protected long endTime;
	protected Map<String, Object> parameters;

	public IndexQueryPlan(Path path, IndexType indexType) {
		super(true, Operator.OperatorType.INDEXQUERY);
		this.indexType = indexType;
		this.paths = new ArrayList<>();
		paths.add(path);
//		this.startTime = startTime;
//		this.endTime = endTime;
	}

	@Override
	public List<Path> getPaths() {
		return paths;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public void setInterval(Pair<Long, Long> interval) {
		setStartTime(interval.left);
		setEndTime(interval.right);
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public abstract Iterator<OnePassQueryDataSet> fetchQueryDateSet(int fetchSize) throws QueryProcessorException;

    public abstract List<String> getColumnHeader();
}
