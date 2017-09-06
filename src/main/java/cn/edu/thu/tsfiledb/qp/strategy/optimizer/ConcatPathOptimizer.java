package cn.edu.thu.tsfiledb.qp.strategy.optimizer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.qp.exception.LogicalOperatorException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
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
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

/**
 * concat paths in select and from clause
 *
 * @author kangrong
 */
public class ConcatPathOptimizer implements ILogicalOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(ConcatPathOptimizer.class);
    private QueryProcessExecutor executor;

    public ConcatPathOptimizer(QueryProcessExecutor executor) {
        this.executor = executor;
    }

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
        return removeStarsInPath(allPaths);
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
        List<Path> concatPaths = new ArrayList<>();
        fromPaths.forEach(fromPath -> concatPaths.add(Path.addPrefixPath(filterPath, fromPath)));
        List<Path> noStarPaths = removeStarsInPathWithUnique(concatPaths);
        if (noStarPaths.size() == 1) {
            // Transform "select s1 from root.car.* where s1 > 10" to
            // "select s1 from root.car.* where root.car.*.s1 > 10"
            basicOperator.setSinglePath(noStarPaths.get(0));
            return operator;
        } else {
            // Transform "select s1 from root.car.d1, root.car.d2 where s1 > 10" to
            // "select s1 from root.car.d1, root.car.d2 where root.car.d1.s1 > 10 and root.car.d2.s1 > 10"
            // Note that, two fork tree has to be maintained while removing stars in paths for DNFFilterOptimizer
            // requirement.
            return constructTwoForkFilterTreeWithAND(noStarPaths, operator);
        }
    }

    private FilterOperator constructTwoForkFilterTreeWithAND(List<Path> noStarPaths, FilterOperator operator)
            throws LogicalOptimizeException {
        FilterOperator filterTwoFolkTree = new FilterOperator(SQLConstant.KW_AND);
        FilterOperator currentNode = filterTwoFolkTree;
        for (int i = 0; i < noStarPaths.size(); i++) {
//            if ((i & 1) == 0 || i == noStarPaths.size() - 1) {
//                try {
//                    currentNode.addChildOperator(new BasicFunctionOperator(operator.getTokenIntType(), noStarPaths
//                            .get(i),
//                            ((BasicFunctionOperator) operator).getValue()));
//                } catch (LogicalOperatorException e) {
//                    throw new LogicalOptimizeException(e.getMessage());
//                }
//            } else {
            if ((i & 1) == 1 && i != noStarPaths.size() - 1) {
                FilterOperator newInnerNode = new FilterOperator(SQLConstant.KW_AND);
                currentNode.addChildOperator(newInnerNode);
                currentNode = newInnerNode;
            }
            try {
                currentNode.addChildOperator(new BasicFunctionOperator(operator.getTokenIntType(), noStarPaths
                        .get(i),
                        ((BasicFunctionOperator) operator).getValue()));
            } catch (LogicalOperatorException e) {
                throw new LogicalOptimizeException(e.getMessage());
            }
        }
        return filterTwoFolkTree;
    }

    /**
     * replace "*" by actual paths
     *
     * @param paths list of paths which may contain stars
     * @return a unique path list
     * @throws LogicalOptimizeException
     */
    private List<Path> removeStarsInPathWithUnique(List<Path> paths) throws LogicalOptimizeException {
        List<Path> retPaths = new ArrayList<>();
        LinkedHashMap<String, Integer> pathMap = new LinkedHashMap<>();
        try {
            for (Path path : paths) {
                List<String> all;
                all = executor.getAllPaths(path.getFullPath());
                for (String subPath : all) {
                    if (!pathMap.containsKey(subPath)) {
                        pathMap.put(subPath, 1);
                    }
                }
            }
            for (String pathStr : pathMap.keySet()) {
                retPaths.add(new Path(pathStr));
            }
        } catch (PathErrorException e) {
            throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
        }
        return retPaths;
    }

    private List<Path> removeStarsInPath(List<Path> paths) throws LogicalOptimizeException {
        List<Path> retPaths = new ArrayList<>();
        for (Path path : paths) {
            try {
                executor.getAllPaths(path.getFullPath()).forEach(p -> retPaths.add(new Path(p)));
            } catch (PathErrorException e) {
                throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
            }
        }
        return retPaths;
    }
}
