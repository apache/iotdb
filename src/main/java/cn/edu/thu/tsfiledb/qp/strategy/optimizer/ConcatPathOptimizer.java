package cn.edu.thu.tsfiledb.qp.strategy.optimizer;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.qp.exception.LogicalOperatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.LogicalOptimizeException;
import cn.edu.thu.tsfiledb.qp.logical.Operator;
import cn.edu.thu.tsfiledb.qp.logical.crud.BasicFunctionOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.FromOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.SFWOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.SelectOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.FunctionOperator;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

/**
 * concat paths in select and from clause
 *
 * @author kangrong
 */
public class ConcatPathOptimizer implements ILogicalOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(ConcatPathOptimizer.class);

    @Override
    public Operator transform(Operator operator) throws LogicalOptimizeException {
        if (!(operator instanceof SFWOperator)) {
            LOG.warn("given operator isn't SFWOperator, cannot concat path");
            return operator;
        }
        SFWOperator sfwOperator = (SFWOperator) operator;
        FromOperator from = sfwOperator.getFromOperator();
        List<Path> prefixPaths;
        if (from == null || (prefixPaths = from.getPrefixPaths()).isEmpty()) {
            LOG.warn("given SFWOperator doesn't have prefix paths, cannot concat path");
            return operator;
        }
        SelectOperator select = sfwOperator.getSelectOperator();
        List<Path> suffixPaths;
        if (select == null || (suffixPaths = select.getSuffixPaths()).isEmpty()) {
            LOG.warn("given SFWOperator doesn't have suffix paths, cannot concat path");
            return operator;
        }
        // concat select paths
        suffixPaths = concatSelect(prefixPaths, suffixPaths);
        select.setSuffixPathList(suffixPaths);
        // concat filter
        FilterOperator filter = sfwOperator.getFilterOperator();
        if (filter == null)
            return operator;
        sfwOperator.setFilterOperator(concatFilter(prefixPaths, filter));
        return sfwOperator;
    }


    private List<Path> concatSelect(List<Path> fromPaths, List<Path> selectPaths)
            throws LogicalOptimizeException {
        List<Path> allPaths = new ArrayList<>();
        for (Path selectPath : selectPaths) {
            if (selectPath.startWith(SQLConstant.ROOT))
                allPaths.add(selectPath);
            else {
                for (Path fromPath : fromPaths) {
                    if (!fromPath.startWith(SQLConstant.ROOT))
                        throw new LogicalOptimizeException("illegal from clause : " + fromPath.getFullPath());
                    allPaths.add(Path.addPrefixPath(selectPath, fromPath));
                }
            }
        }
        return allPaths;
    }

    private FilterOperator concatFilter(List<Path> fromPaths, FilterOperator operator)
            throws LogicalOptimizeException {
        if (!operator.isLeaf()) {
            List<FilterOperator> newFilterList = new ArrayList<>();
            for (FilterOperator child : operator.getChildren()) {
                newFilterList.add(concatFilter(fromPaths, child));
            }
            operator.setChildren(newFilterList);
            return operator;
        }
        BasicFunctionOperator basicOperator = (BasicFunctionOperator) operator;
        Path filterPath = basicOperator.getSinglePath();
        // do nothing in the cases of "where time > 5" or "where root.d1.s1 > 5"
        if (SQLConstant.isReservedPath(filterPath) || filterPath.startWith(SQLConstant.ROOT))
            return operator;
        if (fromPaths.size() == 1) {
            //transfer "select s1 from root.car.* where s1 > 10" to
            // "select s1 from root.car.* where root.car.*.s1 > 10"
            Path newFilterPath = Path.addPrefixPath(filterPath, fromPaths.get(0));
            basicOperator.setSinglePath(newFilterPath);
            return operator;
        } else {
            //transfer "select s1 from root.car.d1, root.car.d2 where s1 > 10" to
            // "select s1 from root.car.d1, root.car.d2 where root.car.d1.s1 > 10 and root.car.d2.s1 > 10"
            FilterOperator newFilter = new FilterOperator(SQLConstant.KW_AND);
            try {
                for (Path fromPath : fromPaths) {
                    Path concatPath = Path.addPrefixPath(filterPath, fromPath);
                    FunctionOperator newFuncOp = new BasicFunctionOperator(operator.getTokenIntType(), concatPath,
                            ((BasicFunctionOperator) operator).getValue());
                    newFilter.addChildOperator(newFuncOp);
                }
            } catch (LogicalOperatorException e) {
                throw new LogicalOptimizeException(e.getMessage());
            }
            return newFilter;
        }
    }
}
