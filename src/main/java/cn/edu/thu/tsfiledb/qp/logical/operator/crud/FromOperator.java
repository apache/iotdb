package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;


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
        prefixList = new ArrayList<Path>();
    }

    public void addPrefixTablePath(Path prefixPath) throws QpSelectFromException {
//        if(!prefixPath.startWith(SQLConstant.ROOT))
//            throw new QpSelectFromException("given select clause path doesn't start with ROOT!");
        prefixList.add(prefixPath);
    }

    // public void addPrefixTablePath(String[] prefixPath) {
    // addPrefixTablePath(new StringContainer(prefixPath, SQLConstant.PATH_SEPARATER));
    // }

    public List<Path> getPrefixPaths() {
        return prefixList;
    }

    /**
     * {@code Pair<path. alias>}
     * 
     * @return not be null, just empty list
     */
    public List<Pair<Path, String>> getPathsAndAlias() {
        // TODO up to now(2016-11-14), we don't involve alias, thus set alias to null
        List<Pair<Path, String>> ret = new ArrayList<Pair<Path, String>>();
        for (Path path: prefixList) {
            ret.add(new Pair<Path, String>(path, null));
        }
        return ret;
    }


}
