package cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter;

import cn.edu.thu.tsfiledb.exception.PathErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.BasicOperatorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpWhereException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.SeriesNotExistException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;

/**
 * basic operator include < > >= <= !=.
 * 
 * @author kangrong
 *
 */

public class BasicFunctionOperator extends FunctionOperator {
    Logger LOG = LoggerFactory.getLogger(BasicFunctionOperator.class);

    private BasicOperatorType funcToken;

    protected Path path;
    protected String value;

    public BasicFunctionOperator(int tokenIntType, Path path, String value)
            throws BasicOperatorException {
        super(tokenIntType);
        operatorType = OperatorType.BASIC_FUNC;
        funcToken = BasicOperatorType.getBasicOpBySymbol(tokenIntType);
        this.path = this.singlePath = path;
        this.value = value;
        isLeaf = true;
        isSingle = true;
    }

    public String getPath() {
        return path.toString();
    }

    public String getValue() {
        return value;
    }

    public void setReversedTokenIntType() throws BasicOperatorException {
        int intType = SQLConstant.reverseWords.get(tokenIntType);
        setTokenIntType(intType);
        funcToken = BasicOperatorType.getBasicOpBySymbol(intType);
    }

    @Override
    public Path getSinglePath() {
        return singlePath;
    }

    @Override
    public void addHeadDeltaObjectPath(String deltaObject) {
        path.addHeadPath(deltaObject);
    }

    @Override
    protected Pair<SingleSeriesFilterExpression, String> transformToSingleFilter(QueryProcessExecutor executor, FilterSeriesType filterType)
            throws QpSelectFromException, QpWhereException, PathErrorException {
        TSDataType type = executor.getSeriesType(path);
        if (type == null) {
            throw new SeriesNotExistException("given path:{" + path.getFullPath()
                    + "} don't exist in metadata");
        }
        SingleSeriesFilterExpression ret;
        switch (type) {
            case INT32:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.intFilterSeries(path.getDeltaObjectToString(),
                                        path.getMeasurementToString(), filterType),
                                Integer.valueOf(value));
                break;
            case INT64:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.longFilterSeries(path.getDeltaObjectToString(),
                                        path.getMeasurementToString(), filterType),
                                Long.valueOf(value));
                break;
            case BOOLEAN:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.booleanFilterSeries(path.getDeltaObjectToString(),
                                        path.getMeasurementToString(), filterType),
                                Boolean.valueOf(value));
                break;
            case FLOAT:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.floatFilterSeries(path.getDeltaObjectToString(),
                                        path.getMeasurementToString(), filterType),
                                Float.valueOf(value));
                break;
            case DOUBLE:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.doubleFilterSeries(path.getDeltaObjectToString(),
                                        path.getMeasurementToString(), filterType),
                                Double.valueOf(value));
                break;
            default:
                throw new QpWhereException("unsupported data type:" + type);
        }
        return new Pair<>(ret, path.getFullPath());
    }

    @Override
    public String showTree(int spaceNum) {
        StringContainer sc = new StringContainer();
        for (int i = 0; i < spaceNum; i++) {
            sc.addTail("  ");
        }
        sc.addTail(path.toString(), this.tokenSymbol, value, ", single\n");
        return sc.toString();
    }

    @Override
    public BasicFunctionOperator clone() {
        BasicFunctionOperator ret;
        try {
            ret = new BasicFunctionOperator(this.tokenIntType, path.clone(), value);
        } catch (BasicOperatorException e) {
            LOG.error("error clone:{}",e.getMessage());
            return null;
        }
        ret.tokenSymbol=tokenSymbol;
        ret.isLeaf = isLeaf;
        ret.isSingle = isSingle;
        return ret;
    }
    
    @Override
    public String toString() {
        return "["+ path.getFullPath()+tokenSymbol+ value +"]";
    }
}
