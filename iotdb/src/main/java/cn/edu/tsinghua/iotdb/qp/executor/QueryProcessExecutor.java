package cn.edu.tsinghua.iotdb.qp.executor;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.ProcessorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.QueryPlan;
import cn.edu.tsinghua.iotdb.query.executor.EngineQueryRouter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.utils.Pair;

import java.io.IOException;
import java.util.*;

public abstract class QueryProcessExecutor {

	protected ThreadLocal<Integer> fetchSize = new ThreadLocal<>();
	private EngineQueryRouter queryRouter = new EngineQueryRouter();

	public QueryProcessExecutor() {
	}

	public QueryDataSet processQuery(PhysicalPlan plan) throws IOException, FileNodeManagerException {
		QueryPlan queryPlan = (QueryPlan) plan;

		QueryExpression queryExpression = QueryExpression.create()
				.setSelectSeries(queryPlan.getPaths())
				.setExpression(queryPlan.getExpression());

		return queryRouter.query(queryExpression);
	}

	public abstract TSDataType getSeriesType(Path fullPath) throws PathErrorException;

	public abstract boolean judgePathExists(Path fullPath);

	public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
		throw new UnsupportedOperationException();
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

	public abstract QueryDataSet aggregate(List<Pair<Path, String>> aggres, IExpression expression)
			throws ProcessorException, IOException, PathErrorException;

	public abstract QueryDataSet groupBy(List<Pair<Path, String>> aggres, IExpression expression,
										 long unit, long origin, List<Pair<Long, Long>> intervals, int fetchSize)
			throws ProcessorException, IOException, PathErrorException;

	/**
	 * executeWithGlobalTimeFilter update command and return whether the operator is successful.
	 *
	 * @param path
	 *            : update series seriesPath
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
	 * executeWithGlobalTimeFilter delete command and return whether the operator is successful.
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
	 * executeWithGlobalTimeFilter delete command and return whether the operator is successful.
	 *
	 * @param path
	 *            : delete series seriesPath
	 * @param deleteTime
	 *            end time in delete command
	 * @return - whether the operator is successful.
	 */
	protected abstract boolean delete(Path path, long deleteTime) throws ProcessorException;

	/**
	 * insert a single value. Only used in test
	 *
	 * @param path
	 *            seriesPath to be inserted
	 * @param insertTime
	 *            - it's time point but not a range
	 * @param value
	 *            value to be inserted
	 * @return - Operate Type.
	 */
	public abstract int insert(Path path, long insertTime, String value) throws ProcessorException;

	/**
	 * executeWithGlobalTimeFilter insert command and return whether the operator is successful.
	 *
	 * @param deviceId
	 *            deviceId to be inserted
	 * @param insertTime
	 *            - it's time point but not a range
	 * @param measurementList
	 *            measurements to be inserted
	 * @param insertValues
	 * 			  values to be inserted
	 * @return - Operate Type.
	 */
	public abstract int multiInsert(String deviceId, long insertTime, List<String> measurementList,
									List<String> insertValues) throws ProcessorException;


	public abstract List<String> getAllPaths(String originPath) throws PathErrorException;

}
