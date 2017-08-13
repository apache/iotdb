package cn.edu.thu.tsfiledb.qp.strategy.optimizer;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.qp.exception.LogicalOptimizeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator;
import cn.edu.thu.tsfiledb.qp.logical.crud.BasicFunctionOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.FromOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.SFWOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.SelectOperator;

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
        concatFilter(prefixPaths, filter);
        sfwOperator.setFilterOperator(filter);
        return sfwOperator;
    }


    private List<Path> concatSelect(List<Path> fromPaths, List<Path> selectPaths)
            throws LogicalOptimizeException {
        if (fromPaths.size() == 1) {
            Path fromPath = fromPaths.get(0);
            if (!fromPath.startWith(SQLConstant.ROOT))
                throw new LogicalOptimizeException("illegal from clause : " + fromPath.getFullPath());
            for (int i = 0;i < selectPaths.size();i++) {
                Path selectPath = selectPaths.get(i);
                if (!selectPath.startWith(SQLConstant.ROOT)) {
                    // add prefix root path
                    selectPaths.set(i, Path.addPrefixPath(selectPath, fromPath));
                }
            }
            return selectPaths;
        } else {
            List<Path> allPaths = new ArrayList<>();
            for (Path selectPath : selectPaths) {
                if(selectPath.startWith(SQLConstant.ROOT))
                    continue;
                for (Path fromPath : fromPaths) {
                    if (!fromPath.startWith(SQLConstant.ROOT))
                        throw new LogicalOptimizeException("illegal from clause : " + fromPath.getFullPath());
                    Path newPath = selectPath.clone();
                    newPath = Path.addPrefixPath(newPath, fromPath);
                    allPaths.add(newPath);
                }
            }
            return allPaths;
        }
    }

    private void concatFilter(List<Path> fromPaths, FilterOperator operator)
            throws LogicalOptimizeException {
        if (!operator.isLeaf()) {
            for (FilterOperator child : operator.getChildren())
                concatFilter(fromPaths, child);
            return;
        }
        BasicFunctionOperator basicOperator = (BasicFunctionOperator) operator;
        Path filterPath = basicOperator.getSinglePath();
        if (SQLConstant.isReservedPath(filterPath))
            return;
        if (fromPaths.size() == 1) {
            Path fromPath = fromPaths.get(0);
            if (!fromPath.startWith(SQLConstant.ROOT))
                throw new LogicalOptimizeException("illegal from clause : " + fromPath.getFullPath());
            if (!filterPath.startWith(SQLConstant.ROOT)) {
                Path newFilterPath = Path.addPrefixPath(filterPath, fromPath);
                basicOperator.setSinglePath(newFilterPath);
                // System.out.println("3===========" + basicOperator.getSinglePath());
            }
            //don't support select s1 from root.car.d1,root.car.d2 where s1 > 10
        } else if (!filterPath.startWith(SQLConstant.ROOT)){
            throw new LogicalOptimizeException("illegal filter path : " + filterPath.getFullPath());
        }
    }
}
