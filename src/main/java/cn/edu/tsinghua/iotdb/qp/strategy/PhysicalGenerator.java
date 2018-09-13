package cn.edu.tsinghua.iotdb.qp.strategy;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.qp.exception.GeneratePhysicalPlanException;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOperatorException;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.executor.OverflowQPExecutor;
import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.*;
import cn.edu.tsinghua.iotdb.qp.logical.index.KvMatchIndexQueryOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.AuthorOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.LoadDataOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.MetadataOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.PropertyOperator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.*;
import cn.edu.tsinghua.iotdb.qp.physical.index.KvMatchIndexQueryPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.AuthorPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.LoadDataPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.MetadataPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.PropertyPlan;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.LongInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.LongFilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(PhysicalGenerator.class);

    public PhysicalGenerator(QueryProcessExecutor executor) {
        this.executor = executor;
    }

    public PhysicalPlan transformToPhysicalPlan(Operator operator) throws QueryProcessorException, ProcessorException {
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
                switch (((IndexQueryOperator) operator).getIndexType()) {
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
            indexQueryPlan.setInterval(new Pair<>(0L, Long.MAX_VALUE));
            return;
        }

        List<Pair<Long, Long>> intervals = extractTimeIntervals(filterOperator);
        if (intervals.size() != 1) {
            throw new LogicalOperatorException("For index query statement, the time filter must be an interval and start time must be less than ene time.");
        }
        indexQueryPlan.setInterval(intervals.get(0));
    }

    /**
     * for update command, time should have start and end time range.
     *
     * @param updateOperator update logical plan
     */
    private void parseUpdateTimeFilter(UpdateOperator updateOperator, UpdatePlan plan) throws LogicalOperatorException {
        List<Pair<Long, Long>> intervals = extractTimeIntervals(updateOperator.getFilterOperator());
        plan.addIntervals(intervals);
        if (plan.getIntervals().isEmpty()) {
            throw new LogicalOperatorException("For update command, time filter is invalid");
        }
    }

    /**
     * extract time intervals from filterOperator
     *
     * @return valid time intervals
     * @throws LogicalOperatorException
     */
    private List<Pair<Long, Long>> extractTimeIntervals(FilterOperator filterOperator) throws LogicalOperatorException {
        List<Pair<Long, Long>> intervals = new ArrayList<>();
        if (!filterOperator.isSingle() || !filterOperator.getSinglePath().equals(RESERVED_TIME)) {
            throw new LogicalOperatorException("filter Operator must be a time filter");
        }
        // transfer the filter operator to FilterExpression
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
                throw new LogicalOperatorException("start and end time must be greater than 0.");
            }
            if (startTime == Long.MIN_VALUE) {
                startTime = 1;
            }

            if (endTime >= startTime)
                intervals.add(new Pair<>(startTime, endTime));
        }
        return intervals;
    }

    private PhysicalPlan transformQuery(QueryOperator queryOperator) throws QueryProcessorException, ProcessorException {

        List<Path> paths = queryOperator.getSelectedPaths();
        List<String> aggregations = queryOperator.getSelectOperator().getAggregations();
        ArrayList<SingleQueryPlan> subPlans = new ArrayList<>();

        FilterOperator filterOperator = queryOperator.getFilterOperator();
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

        if (queryOperator.isGroupBy()) {   //old group by
            multiQueryPlan.setType(MultiQueryPlan.QueryType.GROUPBY);
            multiQueryPlan.setUnit(queryOperator.getUnit());
            multiQueryPlan.setOrigin(queryOperator.getOrigin());
            multiQueryPlan.setIntervals(queryOperator.getIntervals());
            return multiQueryPlan;
        } else if (queryOperator.isFill()) {   // old fill query
            multiQueryPlan.setType(MultiQueryPlan.QueryType.FILL);
            FilterOperator timeFilter = queryOperator.getFilterOperator();
            if (!timeFilter.isSingle())
                throw new QueryProcessorException("Slice query must select a single time point");
            long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
            multiQueryPlan.setQueryTime(time);
            multiQueryPlan.setFillType(queryOperator.getFillTypes());
            return multiQueryPlan;
        } else if (queryOperator.hasAggregation()) { //old aggregation
            return multiQueryPlan;
        } else { //ordinary query
            return transformQueryV2(queryOperator);
        }
    }

    //TODO group by， fill， aggregation 重构完，直接用这个方法替换 transformQuery
    private PhysicalPlan transformQueryV2(QueryOperator queryOperator) throws QueryProcessorException, ProcessorException {

        QueryPlan queryPlan;

        if (queryOperator.isGroupBy()) {
            queryPlan = new GroupByPlan();
            ((GroupByPlan) queryPlan).setUnit(queryOperator.getUnit());
            ((GroupByPlan) queryPlan).setOrigin(queryOperator.getOrigin());
            ((GroupByPlan) queryPlan).setIntervals(queryOperator.getIntervals());
        } else if (queryOperator.isFill()) {
            queryPlan = new FillQueryPlan();
            FilterOperator timeFilter = queryOperator.getFilterOperator();
            if (!timeFilter.isSingle())
                throw new QueryProcessorException("Slice query must select a single time point");
            long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
            ((FillQueryPlan) queryPlan).setQueryTime(time);
            ((FillQueryPlan) queryPlan).setFillType(queryOperator.getFillTypes());
        } else if (queryOperator.hasAggregation()) { // ordinary query
            queryPlan = new AggregationPlan();
            ((AggregationPlan) queryPlan).setAggregations(queryOperator.getSelectOperator().getAggregations());
        } else {
            queryPlan = new QueryPlan();
        }

        List<Path> paths = queryOperator.getSelectedPaths();
        queryPlan.setPaths(paths);
        FilterOperator filterOperator = queryOperator.getFilterOperator();
        if (filterOperator != null) {
            List<FilterOperator> parts = splitFilter(queryOperator.getFilterOperator());
            queryPlan.setQueryFilter(convertDNF2QueryFilter(parts));
        }

        queryPlan.checkPaths(executor);
        return queryPlan;
    }


    private QueryFilter convertDNF2QueryFilter(List<FilterOperator> parts) throws LogicalOperatorException, PathErrorException, GeneratePhysicalPlanException, ProcessorException {
        QueryFilter ret = null;
        List<QueryFilter> queryFilters = new ArrayList<>();
        for (FilterOperator filter : parts) {
            queryFilters.add(convertCNF2QueryFilter(filter));
        }

        for (QueryFilter queryFilter : queryFilters) {
            if (ret == null) {
                ret = queryFilter;
            } else {
                ret = QueryFilterFactory.or(ret, queryFilter);
            }
        }
        return ret;
    }

    private QueryFilter convertCNF2QueryFilter(FilterOperator operator) throws LogicalOperatorException, PathErrorException, GeneratePhysicalPlanException, ProcessorException {

        // e.g. time < 10 and time > 5 ,   time > 10
        if (operator.isSingle() && operator.getSinglePath().toString().equalsIgnoreCase(SQLConstant.RESERVED_TIME)) {
            return new GlobalTimeFilter(convertSingleFilterNode(operator));
        } else {
            if (operator.isSingle()) {  // e.g. s1 > 0 or s1 < 10
                return new SeriesFilter<>(operator.getSinglePath(), convertSingleFilterNode(operator));
            }

            List<FilterOperator> children = operator.getChildren();
            List<SeriesFilter> seriesFilters = new ArrayList<>();
            Filter timeFilter = null;
            List<Pair<Path, Filter>> series2Filters = new ArrayList<>();
            for (FilterOperator child : children) {
                if (!child.isSingle()) {
                    throw new GeneratePhysicalPlanException(
                            "in format:[(a) and () and ()] or [] or [], a is not single! a:" + child);
                }
                Filter currentFilter = convertSingleFilterNode(child);
                if (child.getSinglePath().toString().equalsIgnoreCase(SQLConstant.RESERVED_TIME)) {
                    if (timeFilter != null) {
                        throw new GeneratePhysicalPlanException("time filter has been specified more than once");
                    }
                    timeFilter = currentFilter;
                } else {
                    series2Filters.add(new Pair<>(child.getSinglePath(), currentFilter));
                }
            }

            if (timeFilter == null) {
                for (Pair<Path, Filter> pair : series2Filters) {
                    seriesFilters.add(new SeriesFilter(pair.left, pair.right));
                }
            } else {
                for (Pair<Path, Filter> pair : series2Filters) {
                    seriesFilters.add(new SeriesFilter(pair.left, FilterFactory.and(timeFilter, pair.right)));
                }
            }

            QueryFilter ret = null;
            for (SeriesFilter seriesFilter : seriesFilters) {
                if (ret == null) {
                    ret = seriesFilter;
                } else {
                    ret = QueryFilterFactory.and(ret, seriesFilter);
                }
            }
            return ret;

        }
    }

    private Filter convertSingleFilterNode(FilterOperator node) throws LogicalOperatorException, PathErrorException, ProcessorException {
        if (node.isLeaf()) {
            Path path = node.getSinglePath();
            TSDataType type = executor.getSeriesType(path);
            if (type == null) {
                throw new PathErrorException("given path:{" + path.getFullPath() + "} doesn't exist in metadata");
            }

            BasicFunctionOperator basicOperator = (BasicFunctionOperator) node;
            BasicOperatorType funcToken = BasicOperatorType.getBasicOpBySymbol(node.getTokenIntType());
            boolean isTime = path.equals(SQLConstant.RESERVED_TIME);

            // check value
            String value = basicOperator.getValue();
            if (!path.toString().equalsIgnoreCase(SQLConstant.RESERVED_TIME)) {
                TSDataType dataType = executor.getSeriesType(path);
                value = OverflowQPExecutor.checkValue(dataType, value);
            }

            switch (type) {
                case BOOLEAN:
                    return funcToken.getValueFilter(Boolean.valueOf(value));
                case INT32:
                    return funcToken.getValueFilter(Integer.valueOf(value));
                case INT64:
                    return isTime ? funcToken.getTimeFilter(Long.valueOf(value)) : funcToken.getValueFilter(Long.valueOf(value));
                case FLOAT:
                    return funcToken.getValueFilter(Float.valueOf(value));
                case DOUBLE:
                    return funcToken.getValueFilter(Double.valueOf(value));
                case TEXT:
                    return funcToken.getValueFilter(new Binary(value));
                default:
                    throw new LogicalOperatorException("not supported type: " + type);
            }
        } else {
            int tokenIntType = node.getTokenIntType();
            List<FilterOperator> children = node.getChildren();
            switch (tokenIntType) {
                case KW_AND:
                    return FilterFactory.and(convertSingleFilterNode(children.get(0)), convertSingleFilterNode(children.get(1)));
                case KW_OR:
                    return FilterFactory.or(convertSingleFilterNode(children.get(0)), convertSingleFilterNode(children.get(1)));
                default:
                    throw new LogicalOperatorException("unknown binary tokenIntType:"
                            + tokenIntType + ",maybe it means "
                            + SQLConstant.tokenNames.get(tokenIntType));
            }
        }
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
            throw new GeneratePhysicalPlanException("for one task, filter cannot be OR if it's not single");
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
