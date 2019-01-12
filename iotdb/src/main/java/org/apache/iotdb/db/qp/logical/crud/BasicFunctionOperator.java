package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.StringContainer;

/**
 * basic operator includes < > >= <= !=.
 */
public class BasicFunctionOperator extends FunctionOperator {
    private Logger LOG = LoggerFactory.getLogger(BasicFunctionOperator.class);

    private BasicOperatorType funcToken;

    protected Path path;
    protected String value;

    public BasicFunctionOperator(int tokenIntType, Path path, String value)
            throws LogicalOperatorException {
        super(tokenIntType);
        operatorType = Operator.OperatorType.BASIC_FUNC;
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

    public void setReversedTokenIntType() throws LogicalOperatorException {
        int intType = SQLConstant.reverseWords.get(tokenIntType);
        setTokenIntType(intType);
        funcToken = BasicOperatorType.getBasicOpBySymbol(intType);
    }

    @Override
    public Path getSinglePath() {
        return singlePath;
    }

    @Override
    public void setSinglePath(Path singlePath) {
        this.path = this.singlePath = singlePath;
    }

    @Override
    protected Pair<IUnaryExpression, String> transformToSingleQueryFilter(QueryProcessExecutor executor)
            throws LogicalOperatorException, PathErrorException {
        TSDataType type = executor.getSeriesType(path);
        if (type == null) {
            throw new PathErrorException("given seriesPath:{" + path.getFullPath()
                    + "} don't exist in metadata");
        }
        IUnaryExpression ret;

        switch (type) {
            case INT32:
                ret = funcToken.getUnaryExpression(path, Integer.valueOf(value));
                break;
            case INT64:
                ret = funcToken.getUnaryExpression(path, Long.valueOf(value));
                break;
            case BOOLEAN:
                ret = funcToken.getUnaryExpression(path, Boolean.valueOf(value));
                break;
            case FLOAT:
                ret = funcToken.getUnaryExpression(path, Float.valueOf(value));
                break;
            case DOUBLE:
                ret = funcToken.getUnaryExpression(path, Double.valueOf(value));
                break;
            case TEXT:
                ret = funcToken.getUnaryExpression(path,
                        (value.startsWith("'") && value.endsWith("'")) || (value.startsWith("\"") && value.endsWith("\"")) ?
                                new Binary(value.substring(1, value.length()-1)) : new Binary(value));
                break;
            default:
                throw new LogicalOperatorException("unsupported data type:" + type);
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
        } catch (LogicalOperatorException e) {
            LOG.error("error clone:{}", e.getMessage());
            return null;
        }
        ret.tokenSymbol = tokenSymbol;
        ret.isLeaf = isLeaf;
        ret.isSingle = isSingle;
        return ret;
    }

    @Override
    public String toString() {
        return "[" + path.getFullPath() + tokenSymbol + value + "]";
    }
}
