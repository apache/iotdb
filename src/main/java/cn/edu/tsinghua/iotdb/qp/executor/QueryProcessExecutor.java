package cn.edu.tsinghua.iotdb.qp.executor;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.index.kvmatch.KvMatchQueryRequest;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.executor.iterator.MergeQuerySetIterator;
import cn.edu.tsinghua.iotdb.qp.executor.iterator.PatternQueryDataSetIterator;
import cn.edu.tsinghua.iotdb.qp.executor.iterator.QueryDataSetIterator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.IndexQueryPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.MultiQueryPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.SingleQueryPlan;
import cn.edu.tsinghua.iotdb.query.engine.FilterStructure;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class QueryProcessExecutor {

	protected ThreadLocal<Integer> fetchSize = new ThreadLocal<>();

	public QueryProcessExecutor() {
	}

	public abstract TSDataType getSeriesType(Path fullPath) throws PathErrorException;

	public abstract boolean judgePathExists(Path fullPath);

	//process MultiQueryPlan
	public Iterator<QueryDataSet> processQuery(PhysicalPlan plan) throws QueryProcessorException {
		if(plan instanceof IndexQueryPlan){
			return ((IndexQueryPlan) plan).fetchQueryDateSet(getFetchSize());
		}
		MultiQueryPlan mergeQuery = (MultiQueryPlan) plan;
		List<SingleQueryPlan> selectPlans = mergeQuery.getSingleQueryPlans();
		switch (mergeQuery.getType()) {
			case QUERY:
				if (selectPlans.size() == 1) {
					SingleQueryPlan query = selectPlans.get(0);
					FilterExpression[] filterExpressions = query.getFilterExpressions();
					return new QueryDataSetIterator(query.getPaths(), getFetchSize(),
							this, filterExpressions[0], filterExpressions[1],
							filterExpressions[2]);
				} else {
					return new MergeQuerySetIterator(selectPlans, getFetchSize(), this);
				}
			case AGGREGATION:
				return new QueryDataSetIterator(mergeQuery.getPaths(), getFetchSize(),
						mergeQuery.getAggregations(), getFilterStructure(selectPlans), this);
			case GROUPBY:
				return new QueryDataSetIterator(mergeQuery.getPaths(), getFetchSize(),
						mergeQuery.getAggregations(), getFilterStructure(selectPlans),
						mergeQuery.getUnit(), mergeQuery.getOrigin(), mergeQuery.getIntervals(), this);
			default:
				throw new UnsupportedOperationException();
		}
	}

	public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
		throw new UnsupportedOperationException();
	}

	private List<FilterStructure> getFilterStructure(List<SingleQueryPlan> selectPlans) {
		List<FilterStructure> filterStructures = new ArrayList<>();
		for(SingleQueryPlan selectPlan: selectPlans) {
			FilterExpression[] expressions = selectPlan.getFilterExpressions();
			FilterStructure filterStructure = new FilterStructure(expressions[0], expressions[1], expressions[2]);
			filterStructures.add(filterStructure);
		}
		return filterStructures;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize.set(fetchSize);
	}

	public int getFetchSize() {
		if (fetchSize.get() == null) {
			return 100;
		}
		return fetchSize.get();
	}

	public abstract QueryDataSet aggregate(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures)
			throws ProcessorException, IOException, PathErrorException;

	public abstract QueryDataSet groupBy(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures,
										 long unit, long origin, List<Pair<Long, Long>> intervals, int fetchSize)
			throws ProcessorException, IOException, PathErrorException;

	public abstract QueryDataSet query(int formNumber, List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
			FilterExpression valueFilter, int fetchSize, QueryDataSet lastData) throws ProcessorException;

	/**
	 * execute update command and return whether the operator is successful.
	 * 
	 * @param path
	 *            : update series path
	 * @param startTime
	 *            start time in update command
	 * @param endTime
	 *            end time in update command
	 * @param value
	 *            - in type of string
	 * @return - whether the operator is successful.
	 */
	public abstract boolean update(Path path, long startTime, long endTime, String value) throws ProcessorException;

	/**
	 * execute delete command and return whether the operator is successful.
	 *
	 * @param paths
	 *            : delete series paths
	 * @param deleteTime
	 *            end time in delete command
	 * @return - whether the operator is successful.
	 */
	public boolean delete(List<Path> paths, long deleteTime) throws ProcessorException {
		try {
			boolean result = true;
			MManager mManager = MManager.getInstance();
			Set<String> pathSet = new HashSet<>();
			for (Path p : paths) {
				pathSet.addAll(mManager.getPaths(p.getFullPath()));
			}
			if (pathSet.isEmpty()) {
				throw new ProcessorException("TimeSeries does not exist and cannot be delete data");
			}
			for (String onePath : pathSet) {
				if (!mManager.pathExist(onePath)) {
					throw new ProcessorException(String.format(
							"TimeSeries %s does not exist and cannot be delete its data", onePath));
				}
			}
			List<String> fullPath = new ArrayList<>();
			fullPath.addAll(pathSet);
			for (String path : fullPath) {
				result &= delete(new Path(path), deleteTime);
			}
			return result;
		} catch (PathErrorException e) {
			throw new ProcessorException(e.getMessage());
		}
	}

	/**
	 * execute delete command and return whether the operator is successful.
	 *
	 * @param path
	 *            : delete series path
	 * @param deleteTime
	 *            end time in delete command
	 * @return - whether the operator is successful.
	 */
	protected abstract boolean delete(Path path, long deleteTime) throws ProcessorException;

	/**
	 * insert a single value. Only used in test
	 *
	 * @param path
	 *            path to be inserted
	 * @param insertTime
	 *            - it's time point but not a range
	 * @param value
	 *            value to be inserted
	 * @return - Operate Type.
	 */
	public abstract int insert(Path path, long insertTime, String value) throws ProcessorException;

	/**
	 * execute insert command and return whether the operator is successful.
	 *
	 * @param deltaObject
	 *            deltaObject to be inserted
	 * @param insertTime
	 *            - it's time point but not a range
	 * @param measurementList
	 *            measurements to be inserted
	 * @param insertValues
	 * 			  values to be inserted
	 * @return - Operate Type.
	 */
	public abstract int multiInsert(String deltaObject, long insertTime, List<String> measurementList,
			List<String> insertValues) throws ProcessorException;


	public abstract List<String> getAllPaths(String originPath) throws PathErrorException;

}
