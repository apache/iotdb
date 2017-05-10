package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

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
import cn.edu.thu.tsfiledb.qp.exec.BasicOperatorType;
import cn.edu.thu.tsfiledb.qp.exec.FilterType;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;

/**
 * basic operator include < > >= <= !=.
 * 
 * @author kangrong
 *
 */

public class BasicFunctionOperator extends FunctionOperator {
    Logger LOG = LoggerFactory.getLogger(BasicFunctionOperator.class);

    private BasicOperatorType funcToken;
    private final FilterType filterType;

    protected Path seriesPath;
    protected String seriesValue;

    public String getSeriesPath() {
        return seriesPath.toString();
    }

    public String getSeriesValue() {
        return seriesValue;
    }

    public BasicFunctionOperator(int tokenIntType, Path path, String value)
            throws BasicOperatorException {
        super(tokenIntType);
        operatorType = OperatorType.BASIC_FUNC;
        funcToken = BasicOperatorType.getBasicOpBySymbol(tokenIntType);
        this.seriesPath = this.singlePath = path;
        this.seriesValue = value;
        filterType = FilterType.valueOfStr(seriesPath.getFullPath());

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
        seriesPath.addHeadPath(deltaObject);
    }

    // @Override
    // public Pair<Map<String, FilterOperator>, FilterOperator> splitFilOpWithDNF(
    // Set<String> reservedWords){
    // Map<String, FilterOperator> ret = new HashMap<String, FilterOperator>();
    // if(reservedWords.contains(seriesPath.toString())){
    // ret.put(seriesPath.toString(), this);
    // return new Pair<Map<String,FilterOperator>, FilterOperator>(ret, null);
    // }
    // else {
    // return new Pair<Map<String,FilterOperator>, FilterOperator>(ret, this);
    // }
    // }

    @Override
    protected Pair<SingleSeriesFilterExpression, String> transformToSingleFilter(QueryProcessExecutor exec)
            throws QpSelectFromException, QpWhereException {
        TSDataType type = exec.getSeriesType(seriesPath);
        if (type == null) {
            throw new SeriesNotExistException("given path:{" + seriesPath.getFullPath()
                    + "} don't exist in metadata");
        }
        SingleSeriesFilterExpression ret = null;
        switch (type) {
            case INT32:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.intFilterSeries(seriesPath.getDeltaObjectToString(),
                                        seriesPath.getMeasurementToString(), FilterSeriesType.VALUE_FILTER),
                                Integer.valueOf(seriesValue));
                break;
            case INT64:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.longFilterSeries(seriesPath.getDeltaObjectToString(),
                                        seriesPath.getMeasurementToString(), FilterSeriesType.VALUE_FILTER),
                                Long.valueOf(seriesValue));
                break;
            case BOOLEAN:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.booleanFilterSeries(seriesPath.getDeltaObjectToString(),
                                        seriesPath.getMeasurementToString(), FilterSeriesType.VALUE_FILTER),
                                Boolean.valueOf(seriesValue));
                break;
            case FLOAT:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.floatFilterSeries(seriesPath.getDeltaObjectToString(),
                                        seriesPath.getMeasurementToString(), FilterSeriesType.VALUE_FILTER),
                                Float.valueOf(seriesValue));
                break;
            case DOUBLE:
                ret =
                        funcToken.getSingleSeriesFilterExpression(
                                FilterFactory.doubleFilterSeries(seriesPath.getDeltaObjectToString(),
                                        seriesPath.getMeasurementToString(), FilterSeriesType.VALUE_FILTER),
                                Double.valueOf(seriesValue));
                break;
            default:
                throw new QpWhereException("unsupported data type:" + type);
        }
        return new Pair<SingleSeriesFilterExpression, String>(ret, seriesPath.getFullPath());
    }

    @Override
    public String showTree(int spaceNum) {
        StringContainer sc = new StringContainer();
        for (int i = 0; i < spaceNum; i++) {
            sc.addTail("  ");
        }
        sc.addTail(seriesPath.toString(), this.tokenSymbol, seriesValue, ", single\n");
        return sc.toString();
    }

    @Override
    public BasicFunctionOperator clone() {
        BasicFunctionOperator ret;
        try {
            ret = new BasicFunctionOperator(this.tokenIntType, seriesPath.clone(), seriesValue);
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
        return "["+seriesPath.getFullPath()+tokenSymbol+seriesValue+"]";
    }
}
