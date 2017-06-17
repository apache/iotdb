package cn.edu.thu.tsfiledb.qp.strategy;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.GeneratePhysicalPlanException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.LogicalOperatorException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator;
import cn.edu.thu.tsfiledb.qp.logical.crud.*;
import cn.edu.thu.tsfiledb.qp.logical.sys.AuthorOperator;
import cn.edu.thu.tsfiledb.qp.logical.sys.LoadDataOperator;
import cn.edu.thu.tsfiledb.qp.logical.sys.MetadataOperator;
import cn.edu.thu.tsfiledb.qp.logical.sys.PropertyOperator;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.DeletePlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.MultiInsertPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.UpdatePlan;
import cn.edu.thu.tsfiledb.qp.physical.sys.MetadataPlan;
import cn.edu.thu.tsfiledb.qp.physical.sys.PropertyPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.MergeQuerySetPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.SeriesSelectPlan;
import cn.edu.thu.tsfiledb.qp.physical.sys.AuthorPlan;
import cn.edu.thu.tsfiledb.qp.physical.sys.LoadDataPlan;

import java.util.ArrayList;
import java.util.List;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.*;

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
            case DELETE:
                DeleteOperator delete = (DeleteOperator) operator;
                paths = delete.getSelectedPaths();
                if (paths.size() != 1) {
                    throw new LogicalOperatorException(
                            "for delete command, cannot specified more than one path:" + paths);
                }
                return new DeletePlan(delete.getTime(), paths.get(0));
            case MULTIINSERT:
                MultiInsertOperator multiInsert = (MultiInsertOperator) operator;
                paths = multiInsert.getSelectedPaths();
                if(paths.size() != 1){
                    throw new LogicalOperatorException("for MultiInsert command, cannot specified more than one path:{}"+ paths);
                }
                return new MultiInsertPlan(paths.get(0).getFullPath(), multiInsert.getTime(),
                        multiInsert.getMeasurementList(), multiInsert.getValueList());
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

        }
        return null;
    }


    /**
     * for update command, time should have start and end time range.
     *
     * @param updateOperator update logical plan
     */
    private void parseUpdateTimeFilter(UpdateOperator updateOperator, UpdatePlan plan) throws LogicalOperatorException {
        FilterOperator filterOperator = updateOperator.getFilterOperator();
        if (!filterOperator.isSingle() || !filterOperator.getSinglePath().equals(RESERVED_TIME)) {
            throw new LogicalOperatorException(
                    "for update command, it has non-time condition in where clause");
        }
        if (filterOperator.getChildren().size() != 2)
            throw new LogicalOperatorException(
                    "for update command, time must have left and right boundaries");

        long startTime = -1;
        long endTime = -1;

        for(FilterOperator operator: filterOperator.getChildren()) {
            if(!operator.isLeaf())
                throw new LogicalOperatorException("illegal time condition:" + filterOperator.showTree());
            switch (operator.getTokenIntType()) {
                case LESSTHAN: endTime = Long.valueOf(((BasicFunctionOperator) operator).getValue()) - 1; break;
                case LESSTHANOREQUALTO: endTime = Long.valueOf(((BasicFunctionOperator) operator).getValue()); break;
                case GREATERTHAN:
                    startTime = Long.valueOf(((BasicFunctionOperator) operator).getValue());
                    if(startTime < Long.MAX_VALUE)
                        startTime++; break;
                case GREATERTHANOREQUALTO: startTime = Long.valueOf(((BasicFunctionOperator) operator).getValue()); break;
                default: throw new LogicalOperatorException("time filter must be >,<,>=,<=");
            }
        }

        if (startTime < 0 || endTime < 0) {
            throw new LogicalOperatorException("startTime:" + startTime + ",endTime:" + endTime
                    + ", one of them is illegal");
        }
        if (startTime > endTime) {
            throw new LogicalOperatorException("startTime:" + startTime + ",endTime:" + endTime
                    + ", start time cannot be greater than end time");
        }

        plan.setStartTime(startTime);
        plan.setEndTime(endTime);
    }


    private PhysicalPlan transformQuery(QueryOperator queryOperator) throws QueryProcessorException {
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
            throw new GeneratePhysicalPlanException(
                    "for one tasks, filter cannot be OR if it's not single");
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
                        throw new GeneratePhysicalPlanException(
                                "time filter has been specified more than once");
                    }
                    timeFilter = child;
                    break;
                case SQLConstant.RESERVED_FREQ:
                    if (freqFilter != null) {
                        throw new GeneratePhysicalPlanException(
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
}
