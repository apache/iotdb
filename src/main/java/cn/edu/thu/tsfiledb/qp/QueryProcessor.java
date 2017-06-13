package cn.edu.thu.tsfiledb.qp;

import java.util.Iterator;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.qp.strategy.LogicalGenerator;
import cn.edu.thu.tsfiledb.sql.ParseGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.qp.exception.ErrorQueryOpException;
import cn.edu.thu.tsfiledb.qp.exception.IllegalASTFormatException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.optimize.LogicalOptimizeException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator;
import cn.edu.thu.tsfiledb.qp.logical.root.RootOperator;
import cn.edu.thu.tsfiledb.qp.logical.common.filter.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.root.crud.SFWOperator;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.logical.ConcatPathOptimizer;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.logical.filter.DNFFilterOptimizer;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.logical.filter.MergeSingleFilterOptimizer;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.logical.filter.RemoveNotOptimizer;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.physical.IPhysicalOptimizer;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.physical.NonePhysicalOptimizer;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.ParseException;
import cn.edu.thu.tsfiledb.sql.parse.ParseUtils;

/**
 * provide a integration method for other user.
 * 
 * @author kangrong
 * @author qiaojialin
 *
 */
public class QueryProcessor {
    Logger LOG = LoggerFactory.getLogger(QueryProcessor.class);

    private QueryProcessExecutor executor;


    public QueryProcessor(QueryProcessExecutor executor) {
        this.executor = executor;
    }

    public QueryProcessExecutor getExecutor() {
        return executor;
    }

    public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr)
            throws QueryProcessorException {

        Operator operator = parseSQLToOperator(sqlStr);

        operator = logicalOptimize(operator);

        PhysicalPlan physicalPlan = executor.transformToPhysicalPlan(operator);

        physicalPlan = physicalOptimize(physicalPlan);

        return physicalPlan;
    }

    /**
     * for a query statement, input an SQL statement and return a iterator of {@code QueryDataSet}.
     *
     * @param plan physical plan
     * @return - if parameter op is not in type of QUERY, throw exception. Otherwise, return an
     *         {@linkplain java.util.Iterator Iterator}
     * @throws QueryProcessorException
     */
    public Iterator<QueryDataSet> query(PhysicalPlan plan)
            throws QueryProcessorException {
        if (plan.isQuery()) {
            return executor.processQuery(plan);
        } else {
            throw new ErrorQueryOpException("cannot execute query for a non-query operator:{}"
                    + plan.getOperatorType());
        }
    }

    /**
     * for a non-query statement(insert/update/delete), input an SQL statement and return whether
     * this operator has been successfully.
     *
     * @param plan physical plan
     * @return result
     * @throws ProcessorException
     */
    public boolean nonQuery(PhysicalPlan plan) throws ProcessorException {
        switch (plan.getOperatorType()) {
            case AUTHOR:
            case METADATA:
            case PROPERTY:
            case LOADDATA:
            case MULTIINSERT:
            case UPDATE:
            case DELETE:
                return executor.processNonQuery(plan);
            default:
                throw new ProcessorException("unknown operator type:{}" + plan.getOperatorType());
        }
    }




    /**
     * given a SQL statement and generate a Operator which type maybe {@code SFWOperator} or
     * {@code AuthorOperator}
     * 
     * @param sqlStr - an input SQL statement
     * @return - RootOperator has four subclass:Query/Insert/Delete/Update/Author
     * @throws QueryProcessorException
     * @throws ParseException
     */
    private RootOperator parseSQLToOperator(String sqlStr) throws QueryProcessorException {
        ASTNode astTree;
        // parse string to ASTTree
        try {
            astTree = ParseGenerator.generateAST(sqlStr);
        } catch (ParseException e) {
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr+ " .message:" + e.getMessage());
        }
        astTree = ParseUtils.findRootNonNullToken(astTree);
        LogicalGenerator generator = new LogicalGenerator();
        return generator.getLogicalPlan(astTree);
    }

    /**
     * given an unoptimized logical operator tree and return a optimized result.
     * 
     * @param operator unoptimized logical operator
     * @return optimized logical operator
     * @throws LogicalOptimizeException
     */
    private Operator logicalOptimize(Operator operator) throws LogicalOptimizeException {
        switch (operator.getType()) {
            case AUTHOR:
            case METADATA:
            case PROPERTY:
            case LOADDATA:
            case MULTIINSERT:
                return operator;
            case QUERY:
            case UPDATE:
            case DELETE:
                SFWOperator root = (SFWOperator) operator;
                return optimizeSFWOperator(root);
            default:
                throw new LogicalOptimizeException("unknown operator type:{}" + operator.getType());
        }
    }

    /**
     * given an unoptimized select-from-where operator and return an optimized result.
     *
     * @param root unoptimized
     * @return optimized select-from-where operator
     * @throws LogicalOptimizeException
     */
    private SFWOperator optimizeSFWOperator(SFWOperator root) throws LogicalOptimizeException {
        ConcatPathOptimizer concatPathOptimizer = new ConcatPathOptimizer();
        root = (SFWOperator) concatPathOptimizer.transform(root);
        FilterOperator filter = root.getFilterOperator();
        if (filter == null)
            return root;
        RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
        filter = removeNot.optimize(filter);
        DNFFilterOptimizer dnf = new DNFFilterOptimizer();
        filter = dnf.optimize(filter);
        MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
        filter = merge.optimize(filter);
        root.setFilterOperator(filter);
        return root;
    }


    /**
     * given an unoptimized physical plan and return a optimized result.Up to now, physical
     * optimizer do nothing.
     * 
     * @since 2016-10-11
     * @param physicalPlan physical plan
     * @return
     */
    private PhysicalPlan physicalOptimize(PhysicalPlan physicalPlan) {

            IPhysicalOptimizer physicalOptimizer = new NonePhysicalOptimizer(executor);

            return physicalOptimizer.transform(physicalPlan);
    }

}
