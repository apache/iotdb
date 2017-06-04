package cn.edu.thu.tsfiledb.qp.logical.optimizer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.logical.optimize.PathConcatException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.BasicFunctionOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.FromOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.SFWOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.SelectOperator;



/**
 * This class deals with delta object in union table
 * 
 * @author kangrong
 *
 */
public class ConcatPathOptimizer implements ILogicalOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(ConcatPathOptimizer.class);

    @Override
    public Operator transform(Operator context) throws PathConcatException {
        if (!(context instanceof SFWOperator)) {
            LOG.warn("given operator isn't SFWOperator, cannot concat path");
            return context;
        }
        SFWOperator sfwOperator = (SFWOperator) context;
        FromOperator from = sfwOperator.getFrom();
        List<Pair<Path, String>> prefixPathPairs;
        if (from == null || (prefixPathPairs = from.getPathsAndAlias()).isEmpty()) {
            LOG.warn("given SFWOperator doesn't have prefix paths, cannot concat path");
            return context;
        }
        SelectOperator select = sfwOperator.getSelect();
        List<Path> suffixPaths;
        if (select == null || (suffixPaths = select.getSuffixPaths()).isEmpty()) {
            LOG.warn("given SFWOperator doesn't have suffix paths, cannot concat path");
            return context;
        }
        // concat select paths
        suffixPaths = concatSelect(prefixPathPairs, suffixPaths);
        select.setSuffixPathList(suffixPaths);
        // concat filter
        FilterOperator filter = sfwOperator.getFilter();
        if (filter == null)
            return context;
        concatFilter(prefixPathPairs, filter);
        // sfwOperator.setFilterOperator(filter);
        return sfwOperator;
    }


    private List<Path> concatSelect(List<Pair<Path, String>> fromPaths, List<Path> selectPaths)
            throws PathConcatException {
        if (fromPaths.size() == 1) {
            Pair<Path, String> fromPair = fromPaths.get(0);
            for (Path path : selectPaths) {
                if (path.startWith(fromPair.right)) {
                    // replace alias to namespace path starting with ROOT
                    path.replace(fromPair.right, fromPair.left);
                } else if (!path.startWith(fromPair.left)) {
                    // add prefix root path
                    path.addHeadPath(fromPair.left);
                }
            }
        } else {
            for (Path selectPath : selectPaths) {
                boolean legal = false;
                for (Pair<Path, String> fromPair : fromPaths) {
                    if (selectPath.startWith(fromPair.right)) {
                        // replace alias to namespace path starting with ROOT
                        selectPath.replace(fromPair.right, fromPair.left);
                        legal = true;
                        break;
                    } else if (selectPath.startWith(fromPair.left)) {
                        // do nothing, mark legal
                        legal = true;
                        break;
                    }
                }
                if (!legal) {
                    throw new PathConcatException("select path is illegal:" + selectPath);
                }
            }
        }
        return selectPaths;
    }

    private void concatFilter(List<Pair<Path, String>> fromPaths, FilterOperator filter)
            throws PathConcatException {
        if (!filter.isLeaf()) {
            for (FilterOperator child : filter.getChildren())
                concatFilter(fromPaths, child);
            return;
        }
        BasicFunctionOperator basic = (BasicFunctionOperator) filter;
        Path selectPath = basic.getSinglePath();
        if (SQLConstant.isReservedPath(selectPath))
            return;
        if (fromPaths.size() == 1) {
            Pair<Path, String> fromPair = fromPaths.get(0);
            if (selectPath.startWith(fromPair.right)) {
                // replace alias to namespace path starting with ROOT
                selectPath.replace(fromPair.right, fromPair.left);
            } else if (!selectPath.startWith(fromPair.left)) {
                // add prefix root path
                selectPath.addHeadPath(fromPair.left);
            }

        } else {
            boolean legal = false;
            for (Pair<Path, String> fromPair : fromPaths) {
                if (selectPath.startWith(fromPair.right)) {
                    // replace alias to namespace path starting with ROOT
                    selectPath.replace(fromPair.right, fromPair.left);
                    legal = true;
                    break;
                } else if (selectPath.startWith(fromPair.left)) {
                    // do nothing, mark legal
                    legal = true;
                    break;
                }
            }
            if (!legal) {
                throw new PathConcatException("select path is illegal:" + selectPath);
            }
        }
    }
}
