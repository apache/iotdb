package cn.edu.thu.tsfiledb.qp.strategy;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QueryOperatorException;
import cn.edu.thu.tsfiledb.qp.exception.strategy.TSTransformException;
import cn.edu.thu.tsfiledb.qp.exception.strategy.ParseTimeException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.SelectOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter.BasicFunctionOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.author.AuthorOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.load.LoadDataOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.metadata.MetadataOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.metadata.PropertyOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw.DeleteOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw.MultiInsertOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw.QueryOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw.UpdateOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.*;
import cn.edu.thu.tsfiledb.qp.physical.plan.metadata.MetadataPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.metadata.PropertyPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.query.MergeQuerySetPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.query.SeriesSelectPlan;

import java.util.ArrayList;
import java.util.List;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.*;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.GREATERTHANOREQUALTO;

/**
 * Used to convert logical operator to physical plan
 */
public class Transformer {
    private QueryProcessExecutor executor;

    public Transformer(QueryProcessExecutor executor) {
        this.executor = executor;
    }

    public PhysicalPlan transformToPhysicalPlan(Operator operator) throws QueryProcessorException {
        List<Path> paths;
        switch (operator.getType()) {
            case AUTHOR:
                AuthorOperator author = (AuthorOperator) operator;
                return new AuthorPlan(author.getAuthorType(), author.getUserName(),
                        author.getRoleName(), author.getPassWord(), author.getNewPassword(),
                        author.getPrivilegeList(), author.getNodeName());
            case LOADDATA:
                LoadDataOperator loadData = (LoadDataOperator) operator;
                return new LoadDataPlan(loadData.getInputFilePath(), loadData.getMeasureType());
            case METADATA:
                MetadataOperator metadata = (MetadataOperator) operator;
                return new MetadataPlan(metadata.getNamespaceType(), metadata.getPath(),
                        metadata.getDataType(), metadata.getEncoding(), metadata.getEncodingArgs());
            case PROPERTY:
                PropertyOperator property = (PropertyOperator) operator;
                return new PropertyPlan(property.getPropertyType(), property.getPropertyPath(),
                        property.getMetadataPath());
            case FlUSH: //not support yet
            case MERGE: //not support yet
                throw new TSTransformException("not support plan");
            case DELETE:
                DeleteOperator delete = (DeleteOperator) operator;
                long deleteTime = parseDeleteTimeFilter(delete);
                paths = delete.getSelectedPaths();
                if (paths.size() != 1) {
                    throw new QpSelectFromException(
                            "for delete command, cannot specified more than one path:" + paths);
                }
                return new DeletePlan(deleteTime, paths.get(0));
            case MULTIINSERT:
                MultiInsertOperator multiInsert = (MultiInsertOperator) operator;
                paths = multiInsert.getSelectedPaths();
                if(paths.size() != 1){
                    throw new TSTransformException("for MultiInsert command, cannot specified more than one path:{}"+ paths);
                }
                return new MultiInsertPlan(paths.get(0).getFullPath(), multiInsert.getTime(),
                        multiInsert.getMeasurementList(), multiInsert.getValueList());
            case UPDATE:
                UpdateOperator update = (UpdateOperator) operator;
                UpdatePlan updatePlan = new UpdatePlan();
                parseUpdateTimeFilter(update, updatePlan);
                updatePlan.setValue(update.getValue());
                paths = update.getSelectedPaths();
                if (paths.size() > 1) {
                    throw new QpSelectFromException("update command, must have and only have one path:" + paths);
                }
                updatePlan.setPath(paths.get(0));
                return updatePlan;
            case QUERY:
                QueryOperator query = (QueryOperator) operator;
                return transformQuery(query);

        }
        return null;
    }

    public PhysicalPlan transformQuery(QueryOperator queryOperator) throws QueryProcessorException {
        List<Path> paths = queryOperator.getSelectedPaths();
        SelectOperator selectOperator = queryOperator.getSelectOperator();
        FilterOperator filterOperator = queryOperator.getFilterOperator();

        String aggregation = selectOperator.getAggregation();
        if(aggregation != null)
            executor.addParameter(SQLConstant.IS_AGGREGATION, aggregation);
        ArrayList<SeriesSelectPlan> subPlans = new ArrayList<>();
        if (filterOperator == null) {
            subPlans.add(new SeriesSelectPlan(paths, null, null, null, executor));
        }
        else{
            List<FilterOperator> parts = splitFilter(filterOperator);
            for (FilterOperator filter : parts) {
                SeriesSelectPlan plan = constructSelectPlan(filter, paths, executor);
                if (plan != null)
                    subPlans.add(plan);
            }
        }
        return new MergeQuerySetPlan(subPlans);
    }

    private SeriesSelectPlan constructSelectPlan(FilterOperator filterOperator, List<Path> paths,
                                                 QueryProcessExecutor conf) throws QueryProcessorException {
        FilterOperator timeFilter = null;
        FilterOperator freqFilter = null;
        FilterOperator valueFilter = null;
        List<FilterOperator> singleFilterList;
        if (filterOperator.isSingle()) {
            singleFilterList = new ArrayList<>();
            singleFilterList.add(filterOperator);
        } else if (filterOperator.getTokenIntType() == KW_AND) {
            // now it has been dealt with merge optimizer, thus all nodes with same path have been
            // merged to one node
            singleFilterList = filterOperator.getChildren();
        } else {
            throw new QueryOperatorException(
                    "for one tasks, filter cannot be OR if it's not single");
        }
        List<FilterOperator> valueList = new ArrayList<>();
        for (FilterOperator child : singleFilterList) {
            if (!child.isSingle()) {
                throw new QueryOperatorException(
                        "in format:[(a) and () and ()] or [] or [], a is not single! a:" + child);
            }
            switch (child.getSinglePath().toString()) {
                case SQLConstant.RESERVED_TIME:
                    if (timeFilter != null) {
                        throw new QueryOperatorException(
                                "time filter has been specified more than once");
                    }
                    timeFilter = child;
                    break;
                case SQLConstant.RESERVED_FREQ:
                    if (freqFilter != null) {
                        throw new QueryOperatorException(
                                "freq filter has been specified more than once");
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

        return new SeriesSelectPlan(paths, timeFilter, freqFilter, valueFilter, conf);
    }

    /**
     * split filter operator to a list of filter with relation of "or" each other.
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


    /**
     * for delete command, time should only have an end time.
     *
     * @param operator delete logical plan
     */
    private long parseDeleteTimeFilter(DeleteOperator operator) throws ParseTimeException {
        FilterOperator filterOperator= operator.getFilterOperator();
        if (!(filterOperator.isLeaf())) {
            throw new ParseTimeException(
                    "for delete command, where clause must be like : time < XXX or time <= XXX");
        }

        if (filterOperator.getTokenIntType() != LESSTHAN
                && filterOperator.getTokenIntType() != LESSTHANOREQUALTO) {
            throw new ParseTimeException(
                    "for delete command, time filter must be less than or less than or equal to, this:"
                            + filterOperator.getTokenIntType());
        }
        long time = Long.valueOf(((BasicFunctionOperator) filterOperator).getValue());

        if (time < 0) {
            throw new ParseTimeException("delete Time:" + time + ", time must >= 0");
        }
        return time;
    }

    /**
     * for update command, time should have start and end time range.
     *
     * @param upPlan
     */
    private void parseUpdateTimeFilter(UpdateOperator updateOperator, UpdatePlan upPlan) throws ParseTimeException {
        FilterOperator filterOperator = updateOperator.getFilterOperator();
        if (!filterOperator.isSingle() || !filterOperator.getSinglePath().equals(RESERVED_TIME)) {
            throw new ParseTimeException(
                    "for update command, it has non-time condition in where clause");
        }
        if (filterOperator.getChildren().size() != 2)
            throw new ParseTimeException(
                    "for update command, time must have left and right boundaries");

        long startTime = -1;
        long endTime = -1;

        for(FilterOperator operator: filterOperator.getChildren()) {
            if(!operator.isLeaf())
                throw new ParseTimeException("illegal time condition:" + filterOperator.showTree());
            switch (operator.getTokenIntType()) {
                case LESSTHAN: endTime = Long.valueOf(((BasicFunctionOperator) operator).getValue()) - 1; break;
                case LESSTHANOREQUALTO: endTime = Long.valueOf(((BasicFunctionOperator) operator).getValue()); break;
                case GREATERTHAN:
                    startTime = Long.valueOf(((BasicFunctionOperator) operator).getValue());
                    if(startTime < Long.MAX_VALUE)
                        startTime++; break;
                case GREATERTHANOREQUALTO: startTime = Long.valueOf(((BasicFunctionOperator) operator).getValue()); break;
                default: throw new ParseTimeException("time filter must be >,<,>=,<=");
            }
        }

        if (startTime < 0 || endTime < 0) {
            throw new ParseTimeException("startTime:" + startTime + ",endTime:" + endTime
                    + ", one of them is illegal");
        }
        if (startTime > endTime) {
            throw new ParseTimeException("startTime:" + startTime + ",endTime:" + endTime
                    + ", start time cannot be greater than end time");
        }
        upPlan.setStartTime(startTime);
        upPlan.setEndTime(endTime);
    }
}
