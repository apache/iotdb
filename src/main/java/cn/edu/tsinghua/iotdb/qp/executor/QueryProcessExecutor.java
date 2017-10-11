package cn.edu.tsinghua.iotdb.qp.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.dao.Authorizer;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.executor.iterator.MergeQuerySetIterator;
import cn.edu.tsinghua.iotdb.qp.executor.iterator.QueryDataSetIterator;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.MergeQuerySetPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.SeriesSelectPlan;
import cn.edu.tsinghua.iotdb.qp.strategy.PhysicalGenerator;
import cn.edu.tsinghua.iotdb.query.engine.FilterStructure;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;

public abstract class QueryProcessExecutor {

	protected ThreadLocal<Map<String, Object>> parameters = new ThreadLocal<>();
	protected ThreadLocal<Integer> fetchSize = new ThreadLocal<>();

	public QueryProcessExecutor() {
	}

	protected abstract TSDataType getNonReservedSeriesType(Path fullPath) throws PathErrorException;

	protected abstract boolean judgeNonReservedPathExists(Path fullPath);


	public Iterator<QueryDataSet> processQuery(PhysicalPlan plan) throws QueryProcessorException {
		switch (plan.getOperatorType()) {
			case QUERY:
				//query and aggregate
				SeriesSelectPlan query = (SeriesSelectPlan) plan;
				FilterExpression[] filterExpressions = query.getFilterExpressions();
				return new QueryDataSetIterator(query.getPaths(), getFetchSize(),
						this, filterExpressions[0], filterExpressions[1],
						filterExpressions[2], query.getAggregations());
			case MERGEQUERY:
				MergeQuerySetPlan mergeQuery = (MergeQuerySetPlan) plan;
				List<SeriesSelectPlan> selectPlans = mergeQuery.getSeriesSelectPlans();
				//query
				if (mergeQuery.getAggregations().isEmpty()) {
					return new MergeQuerySetIterator(selectPlans, getFetchSize(), this);
				} else {
					//aggregate
					List<FilterStructure> filterStructures = new ArrayList<>();
					for(SeriesSelectPlan selectPlan: selectPlans) {
						FilterExpression[] expressions = selectPlan.getFilterExpressions();
						FilterStructure filterStructure = new FilterStructure(expressions[0], expressions[1], expressions[2]);
						filterStructures.add(filterStructure);
					}
					return new QueryDataSetIterator(mergeQuery.getPaths(), getFetchSize(),
							mergeQuery.getAggregations(), filterStructures, this);
				}
			default:
				throw new UnsupportedOperationException();
		}
	}

	public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
		throw new UnsupportedOperationException();
	}

	public TSDataType getSeriesType(Path fullPath) throws PathErrorException {
		if (fullPath.equals(SQLConstant.RESERVED_TIME))
			return TSDataType.INT64;
		if (fullPath.equals(SQLConstant.RESERVED_FREQ))
			return TSDataType.FLOAT;
		return getNonReservedSeriesType(fullPath);
	}

	public boolean judgePathExists(Path pathStr) {
		if (SQLConstant.isReservedPath(pathStr))
			return true;
		else
			return judgeNonReservedPathExists(pathStr);
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
				throw new ProcessorException(
						String.format("Timeseries does not exist and cannot be delete data"));
			}
			for (String onePath : pathSet) {
				if (!mManager.pathExist(onePath)) {
					throw new ProcessorException(String.format(
							"Timeseries %s does not exist and cannot be delete its data", onePath));
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
	public abstract boolean delete(Path path, long deleteTime) throws ProcessorException;

	/**
	 * execute insert command and return whether the operator is successful.
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

	public abstract int multiInsert(String deltaObject, long insertTime, List<String> measurementList,
			List<String> insertValues) throws ProcessorException;

	public MManager getMManager() {
		return MManager.getInstance();
	}

	public void addParameter(String key, Object value) {
		if (parameters.get() == null) {
			parameters.set(new HashMap<>());
		}
		parameters.get().put(key, value);
	}

	public Object getParameter(String key) {
		return parameters.get().get(key);
	}

	public void clearParameters() {
		if (parameters.get() != null) {
			parameters.get().clear();
		}
		if (fetchSize.get() != null) {
			fetchSize.remove();
		}
	}

	/**
	 *
	 * @param username
	 *            updated user's name
	 * @param newPassword
	 *            new password
	 * @return boolean
	 * @throws AuthException
	 *             exception in update user
	 */
	public boolean updateUser(String username, String newPassword) throws AuthException {
		return Authorizer.updateUserPassword(username, newPassword);
	}

	public boolean createUser(String username, String password) throws AuthException {
		return Authorizer.createUser(username, password);
	}

	public boolean addPermissionToUser(String userName, String nodeName, int permissionId) throws AuthException {
		return Authorizer.addPmsToUser(userName, nodeName, permissionId);
	}

	public boolean removePermissionFromUser(String userName, String nodeName, int permissionId) throws AuthException {
		return Authorizer.removePmsFromUser(userName, nodeName, permissionId);
	}

	public boolean deleteUser(String userName) throws AuthException {
		return Authorizer.deleteUser(userName);
	}

	public boolean createRole(String roleName) throws AuthException {
		return Authorizer.createRole(roleName);
	}

	public boolean addPermissionToRole(String roleName, String nodeName, int permissionId) throws AuthException {
		return Authorizer.addPmsToRole(roleName, nodeName, permissionId);
	}

	public boolean removePermissionFromRole(String roleName, String nodeName, int permissionId) throws AuthException {
		return Authorizer.removePmsFromRole(roleName, nodeName, permissionId);
	}

	public boolean deleteRole(String roleName) throws AuthException {
		return Authorizer.deleteRole(roleName);
	}

	public boolean grantRoleToUser(String roleName, String username) throws AuthException {
		return Authorizer.grantRoleToUser(roleName, username);
	}

	public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
		return Authorizer.revokeRoleFromUser(roleName, username);
	}

	public Set<Integer> getPermissionsOfUser(String username, String nodeName) throws AuthException {
		return Authorizer.getPermission(username, nodeName);
	}

	public PhysicalPlan queryPhysicalOptimize(PhysicalPlan plan) {
		return plan;
	}

	public PhysicalPlan nonQueryPhysicalOptimize(PhysicalPlan plan) {
		return plan;
	}

	public abstract List<String> getAllPaths(String originPath) throws PathErrorException;

}
