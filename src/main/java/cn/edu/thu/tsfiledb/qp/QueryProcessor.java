package cn.edu.thu.tsfiledb.qp;

import cn.edu.thu.tsfiledb.qp.exception.LogicalOperatorException;
import cn.edu.thu.tsfiledb.qp.strategy.LogicalGenerator;
import cn.edu.thu.tsfiledb.sql.ParseGenerator;

import org.joda.time.DateTimeZone;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.exception.IllegalASTFormatException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.LogicalOptimizeException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator;
import cn.edu.thu.tsfiledb.qp.logical.RootOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.SFWOperator;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.ConcatPathOptimizer;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.DNFFilterOptimizer;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.MergeSingleFilterOptimizer;
import cn.edu.thu.tsfiledb.qp.strategy.optimizer.RemoveNotOptimizer;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.ParseException;
import cn.edu.thu.tsfiledb.sql.parse.ParseUtils;

/**
 * provide a integration method for other user.
 *
 * @author kangrong
 * @author qiaojialin
 */
public class QueryProcessor {

    private QueryProcessExecutor executor;
    
    public QueryProcessor(QueryProcessExecutor executor) {
        this.executor = executor;
    }

    public QueryProcessExecutor getExecutor() {
        return executor;
    }

    public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr)
            throws QueryProcessorException, ArgsErrorException {
    		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        return parseSQLToPhysicalPlan(sqlStr, config.timeZone);
    }
    
    public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, DateTimeZone timeZone)
            throws QueryProcessorException, ArgsErrorException {
        ASTNode astNode = parseSQLToAST(sqlStr);
        Operator operator = parseASTToOperator(astNode, timeZone);
        operator = logicalOptimize(operator);
        return executor.transformToPhysicalPlan(operator);
    }

    /**
     * Convert ast tree to Operator which type maybe {@code SFWOperator} or
     * {@code AuthorOperator}
     *
     * @param astNode - input ast tree
     * @return - RootOperator has four subclass:Query/Insert/Delete/Update/Author
     * @throws QueryProcessorException exception in converting sql to operator
     * @throws ArgsErrorException 
     */
    private RootOperator parseASTToOperator(ASTNode astNode, DateTimeZone timeZone) throws QueryProcessorException, ArgsErrorException {
        LogicalGenerator generator = new LogicalGenerator(timeZone);
        return generator.getLogicalPlan(astNode);
    }

    /**
     * Given a SQL statement and generate an ast tree
     *
     * @param sqlStr input sql command
     * @return ast tree
     * @throws IllegalASTFormatException exception in sql parsing
     */
    private ASTNode parseSQLToAST(String sqlStr) throws IllegalASTFormatException {
        ASTNode astTree;
        // parse string to ASTTree
        try {
            astTree = ParseGenerator.generateAST(sqlStr);
        } catch (ParseException e) {
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        return ParseUtils.findRootNonNullToken(astTree);
    }

    /**
     * given an unoptimized logical operator tree and return a optimized result.
     *
     * @param operator unoptimized logical operator
     * @return optimized logical operator
     * @throws LogicalOptimizeException exception in logical optimizing
     */
    private Operator logicalOptimize(Operator operator) throws LogicalOperatorException {
        switch (operator.getType()) {
            case AUTHOR:
            case METADATA:
            case PROPERTY:
            case LOADDATA:
            case INSERT:
            case INDEX:
                return operator;
            case QUERY:
            case UPDATE:
            case DELETE:
                SFWOperator root = (SFWOperator) operator;
                return optimizeSFWOperator(root);
            default:
                throw new LogicalOperatorException("unknown operator type:{}" + operator.getType());
        }
    }

    /**
     * given an unoptimized select-from-where operator and return an optimized result.
     *
     * @param root unoptimized select-from-where operator
     * @return optimized select-from-where operator
     * @throws LogicalOptimizeException exception in SFW optimizing
     */
    private SFWOperator optimizeSFWOperator(SFWOperator root) throws LogicalOperatorException {
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

}
