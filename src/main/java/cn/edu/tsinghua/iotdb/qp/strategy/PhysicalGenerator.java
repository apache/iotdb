package cn.edu.tsinghua.iotdb.qp.strategy;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.qp.exception.GeneratePhysicalPlanException;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOperatorException;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.BasicFunctionOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.DeleteOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.FilterOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.IndexOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.IndexQueryOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.InsertOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.QueryOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.SelectOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.UpdateOperator;
import cn.edu.tsinghua.iotdb.qp.logical.index.KvMatchIndexQueryOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.AuthorOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.LoadDataOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.MetadataOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.PropertyOperator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.IndexPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.IndexQueryPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.MultiQueryPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.SingleQueryPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.qp.physical.index.KvMatchIndexQueryPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.AuthorPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.LoadDataPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.MetadataPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.PropertyPlan;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.LongInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.LongFilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iotdb.qp.constant.SQLConstant.KW_AND;
import static cn.edu.tsinghua.iotdb.qp.constant.SQLConstant.KW_OR;
import static cn.edu.tsinghua.iotdb.qp.constant.SQLConstant.RESERVED_TIME;

/**
 * Used to convert logical operator to physical plan
 */
public class PhysicalGenerator {
	private QueryProcessExecutor executor;

	public PhysicalGenerator(QueryProcessExecutor executor) {
		this.executor = executor;
	}

	public PhysicalPlan transformToPhysicalPlan(Operator operator) throws QueryProcessorException {
		List<Path> paths;
		switch (operator.getType()) {
		case AUTHOR:
			AuthorOperator author = (AuthorOperator) operator;
			try {
				return new AuthorPlan(author.getAuthorType(), author.getUserName(), author.getRoleName(),
                        author.getPassWord(), author.getNewPassword(), author.getPrivilegeList(), author.getNodeName());
			} catch (AuthException e) {
				throw new QueryProcessorException(e.getMessage());
			}
		case LOADDATA:
			LoadDataOperator loadData = (LoadDataOperator) operator;
			return new LoadDataPlan(loadData.getInputFilePath(), loadData.getMeasureType());
		case SET_STORAGE_GROUP:
		case DELETE_TIMESERIES:
		case METADATA:
			MetadataOperator metadata = (MetadataOperator) operator;
			return new MetadataPlan(metadata.getNamespaceType(), metadata.getPath(), metadata.getDataType(),
					metadata.getEncoding(), metadata.getEncodingArgs(), metadata.getDeletePathList());
		case PROPERTY:
			PropertyOperator property = (PropertyOperator) operator;
			return new PropertyPlan(property.getPropertyType(), property.getPropertyPath(), property.getMetadataPath());
		case DELETE:
			DeleteOperator delete = (DeleteOperator) operator;
			paths = delete.getSelectedPaths();
			if (delete.getTime() <= 0) {
				throw new LogicalOperatorException("For Delete command, time must greater than 0.");
			}
			return new DeletePlan(delete.getTime(), paths);
		case INSERT:
			InsertOperator Insert = (InsertOperator) operator;
			paths = Insert.getSelectedPaths();
			if (paths.size() != 1) {
				throw new LogicalOperatorException(
						"For Insert command, cannot specified more than one path:" + paths);
			}
			if (Insert.getTime() <= 0) {
				throw new LogicalOperatorException("For Insert command, time must greater than 0.");
			}
			return new InsertPlan(paths.get(0).getFullPath(), Insert.getTime(), Insert.getMeasurementList(),
					Insert.getValueList());
		case UPDATE:
			UpdateOperator update = (UpdateOperator) operator;
			UpdatePlan updatePlan = new UpdatePlan();
			updatePlan.setValue(update.getValue());
			paths = update.getSelectedPaths();
			if (paths.size() > 1) {
				throw new LogicalOperatorException("update command, must have and only have one path:" + paths);
			}
			updatePlan.setPath(paths.get(0));
			parseUpdateTimeFilter(update, updatePlan);
			return updatePlan;
		case QUERY:
			QueryOperator query = (QueryOperator) operator;
			return transformQuery(query);
		case INDEX:
			IndexOperator indexOperator = (IndexOperator) operator;
			return new IndexPlan(indexOperator.getPath(), indexOperator.getParameters(),
					indexOperator.getStartTime(), indexOperator.getIndexOperatorType(), indexOperator.getIndexType());
		case INDEXQUERY:
			switch (((IndexQueryOperator) operator).getIndexType()){
				case KvIndex:
					KvMatchIndexQueryOperator indexQueryOperator = (KvMatchIndexQueryOperator) operator;
					KvMatchIndexQueryPlan indexQueryPlan = new KvMatchIndexQueryPlan(indexQueryOperator.getPath(),
							indexQueryOperator.getPatternPath(), indexQueryOperator.getEpsilon(),
							indexQueryOperator.getStartTime(), indexQueryOperator.getEndTime());
					indexQueryPlan.setAlpha(indexQueryOperator.getAlpha());
					indexQueryPlan.setBeta(indexQueryOperator.getBeta());
					parseIndexTimeFilter(indexQueryOperator, indexQueryPlan);
					return indexQueryPlan;
				default:
					throw new LogicalOperatorException("not support index type:" + ((IndexQueryOperator) operator).getIndexType());
			}
		default:
			throw new LogicalOperatorException("not supported operator type: " + operator.getType());
		}
	}

	private void parseIndexTimeFilter(IndexQueryOperator indexQueryOperator, IndexQueryPlan indexQueryPlan)
			throws LogicalOperatorException {
		FilterOperator filterOperator = indexQueryOperator.getFilterOperator();
		if (filterOperator == null) {
			indexQueryPlan.setStartTime(0);
			indexQueryPlan.setEndTime(Long.MAX_VALUE);
			return;
		}
		if (!filterOperator.isSingle() || !filterOperator.getSinglePath().equals(RESERVED_TIME)) {
			throw new LogicalOperatorException("For index query statement, non-time condition is not allowed in the where clause.");
		}
		FilterExpression timeFilter;
		try {
			timeFilter = filterOperator.transformToFilterExpression(executor, FilterSeriesType.TIME_FILTER);
		} catch (QueryProcessorException e) {
			e.printStackTrace();
			throw new LogicalOperatorException(e.getMessage());
		}
		LongFilterVerifier filterVerifier = (LongFilterVerifier) FilterVerifier.create(TSDataType.INT64);
		LongInterval longInterval = filterVerifier.getInterval((SingleSeriesFilterExpression) timeFilter);
		long startTime;
		long endTime;
		if (longInterval.count != 2) {
			throw new LogicalOperatorException("For index query statement, the time filter must be an interval.");
		}
		if (longInterval.flag[0]) {
			startTime = longInterval.v[0];
		} else {
			startTime = longInterval.v[0] + 1;
		}
		if (longInterval.flag[1]) {
			endTime = longInterval.v[1];
		} else {
			endTime = longInterval.v[1] - 1;
		}
		if ((startTime <= 0 && startTime != Long.MIN_VALUE) || endTime <= 0) {
			throw new LogicalOperatorException("The time of index query must be greater than 0.");
		}
		if (startTime == Long.MIN_VALUE) {
			startTime = 0;
		}
		if (endTime >= startTime) {
			indexQueryPlan.setStartTime(startTime);
			indexQueryPlan.setEndTime(endTime);
		} else {
			throw new LogicalOperatorException("For index query statement, the start time should be greater than end time.");
		}
	}

	/**
	 * for update command, time should have start and end time range.
	 *
	 * @param updateOperator
	 *            update logical plan
	 */
	private void parseUpdateTimeFilter(UpdateOperator updateOperator, UpdatePlan plan) throws LogicalOperatorException {
		FilterOperator filterOperator = updateOperator.getFilterOperator();
		if (!filterOperator.isSingle() || !filterOperator.getSinglePath().equals(RESERVED_TIME)) {
			throw new LogicalOperatorException("for update command, it has non-time condition in where clause");
		}
		// transfer the filter operator to FilterExpression
		FilterExpression timeFilter;
		try {
			timeFilter = filterOperator.transformToFilterExpression(executor, FilterSeriesType.TIME_FILTER);
		} catch (QueryProcessorException e) {
			e.printStackTrace();
			throw new LogicalOperatorException(e.getMessage());
		}
		LongFilterVerifier filterVerifier = (LongFilterVerifier) FilterVerifier
				.create(TSDataType.INT64);
		LongInterval longInterval = filterVerifier
				.getInterval((SingleSeriesFilterExpression) timeFilter);
		long startTime;
		long endTime;
		for (int i = 0; i < longInterval.count; i = i + 2) {
			if (longInterval.flag[i]) {
				startTime = longInterval.v[i];
			} else {
				startTime = longInterval.v[i] + 1;
			}
			if (longInterval.flag[i + 1]) {
				endTime = longInterval.v[i + 1];
			} else {
				endTime = longInterval.v[i + 1] - 1;
			}
			if ((startTime <= 0 && startTime != Long.MIN_VALUE) || endTime <= 0) {
				throw new LogicalOperatorException("update time must be greater than 0.");
			}
			if (startTime == Long.MIN_VALUE) {
				startTime = 1;
			}

			if (endTime >= startTime)
				plan.addInterval(new Pair<>(startTime, endTime));
		}
		if (plan.getIntervals().isEmpty()) {
			throw new LogicalOperatorException("For update command, time filter is invalid");
		}
	}

	private PhysicalPlan transformQuery(QueryOperator queryOperator) throws QueryProcessorException {
		List<Path> paths = queryOperator.getSelectedPaths();
		SelectOperator selectOperator = queryOperator.getSelectOperator();
		FilterOperator filterOperator = queryOperator.getFilterOperator();

		List<String> aggregations = selectOperator.getAggregations();
		ArrayList<SingleQueryPlan> subPlans = new ArrayList<>();
		if (filterOperator == null) {
			subPlans.add(new SingleQueryPlan(paths, null, null, null, executor, null));
		} else {
			List<FilterOperator> parts = splitFilter(filterOperator);
			for (FilterOperator filter : parts) {
				SingleQueryPlan plan = constructSelectPlan(filter, paths, executor);
				subPlans.add(plan);
			}
		}

		MultiQueryPlan multiQueryPlan = new MultiQueryPlan(subPlans, aggregations);

		if(queryOperator.isGroupBy()) {
			multiQueryPlan.setType(MultiQueryPlan.QueryType.GROUPBY);
			multiQueryPlan.setUnit(queryOperator.getUnit());
			multiQueryPlan.setOrigin(queryOperator.getOrigin());
			multiQueryPlan.setIntervals(queryOperator.getIntervals());
		}

		if(queryOperator.isFill()) {
			multiQueryPlan.setType(MultiQueryPlan.QueryType.FILL);
			FilterOperator timeFilter = queryOperator.getFilterOperator();
			if(!timeFilter.isSingle())
				throw new QueryProcessorException("Slice query must select a single time point");
			long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
			multiQueryPlan.setQueryTime(time);
			multiQueryPlan.setFillType(queryOperator.getFillTypes());
		}

		return multiQueryPlan;
	}

	private SingleQueryPlan constructSelectPlan(FilterOperator filterOperator, List<Path> paths,
												QueryProcessExecutor conf) throws QueryProcessorException {
		FilterOperator timeFilter = null;
		FilterOperator freqFilter = null;
		FilterOperator valueFilter = null;
		List<FilterOperator> singleFilterList;
		if (filterOperator.isSingle()) {
			singleFilterList = new ArrayList<>();
			singleFilterList.add(filterOperator);
		} else if (filterOperator.getTokenIntType() == KW_AND) {
			// now it has been dealt with merge optimizer, thus all nodes with
			// same path have been merged to one node
			singleFilterList = filterOperator.getChildren();
		} else {
			throw new GeneratePhysicalPlanException("for one tasks, filter cannot be OR if it's not single");
		}
		List<FilterOperator> valueList = new ArrayList<>();
		for (FilterOperator child : singleFilterList) {
			if (!child.isSingle()) {
				throw new GeneratePhysicalPlanException(
						"in format:[(a) and () and ()] or [] or [], a is not single! a:" + child);
			}
			switch (child.getSinglePath().toString()) {
				case SQLConstant.RESERVED_TIME:
					if (timeFilter != null) {
						throw new GeneratePhysicalPlanException("time filter has been specified more than once");
					}
					timeFilter = child;
					break;
				case SQLConstant.RESERVED_FREQ:
					if (freqFilter != null) {
						throw new GeneratePhysicalPlanException("freq filter has been specified more than once");
					}
					freqFilter = child;
					break;
				default:
					valueList.add(child);
					break;
			}
		}
		if (valueList.size() == 1) {
			valueFilter = valueList.get(0);
		} else if (valueList.size() > 1) {
			valueFilter = new FilterOperator(KW_AND, false);
			valueFilter.setChildren(valueList);
		}

		return new SingleQueryPlan(paths, timeFilter, freqFilter, valueFilter, conf, null);
	}

	/**
	 * split filter operator to a list of filter with relation of "or" each
	 * other.
	 *
	 */
	private List<FilterOperator> splitFilter(FilterOperator filterOperator) {
		List<FilterOperator> ret = new ArrayList<>();
		if (filterOperator.isSingle() || filterOperator.getTokenIntType() != KW_OR) {
			// single or leaf(BasicFunction)
			ret.add(filterOperator);
			return ret;
		}
		// a list of partion linked with or
		return filterOperator.getChildren();
	}

}
