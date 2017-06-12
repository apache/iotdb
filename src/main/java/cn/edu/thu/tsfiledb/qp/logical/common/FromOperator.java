package cn.edu.thu.tsfiledb.qp.logical.common;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.qp.logical.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;


/**
 * this class maintains information from {@code FROM} clause
 * 
 * @author kangrong
 *
 */
public class FromOperator extends Operator {

    Logger LOG = LoggerFactory.getLogger(FromOperator.class);
    private List<Path> prefixList;

    public FromOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.FROM;
        prefixList = new ArrayList<>();
    }

    public void addPrefixTablePath(Path prefixPath) throws QpSelectFromException {
        prefixList.add(prefixPath);
    }

    public List<Path> getPrefixPaths() {
        return prefixList;
    }

}
