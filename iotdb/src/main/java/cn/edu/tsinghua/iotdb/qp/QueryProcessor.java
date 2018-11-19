package cn.edu.tsinghua.iotdb.qp;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.qp.exception.IllegalASTFormatException;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOperatorException;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOptimizeException;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.logical.RootOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.FilterOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.SFWOperator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.strategy.LogicalGenerator;
import cn.edu.tsinghua.iotdb.qp.strategy.PhysicalGenerator;
import cn.edu.tsinghua.iotdb.qp.strategy.optimizer.ConcatPathOptimizer;
import cn.edu.tsinghua.iotdb.qp.strategy.optimizer.DNFFilterOptimizer;
import cn.edu.tsinghua.iotdb.qp.strategy.optimizer.MergeSingleFilterOptimizer;
import cn.edu.tsinghua.iotdb.qp.strategy.optimizer.RemoveNotOptimizer;
import cn.edu.tsinghua.iotdb.sql.ParseGenerator;
import cn.edu.tsinghua.iotdb.sql.parse.ASTNode;
import cn.edu.tsinghua.iotdb.sql.parse.ParseException;
import cn.edu.tsinghua.iotdb.sql.parse.ParseUtils;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import org.joda.time.DateTimeZone;

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
            throws QueryProcessorException, ArgsErrorException, ProcessorException {
    		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        return parseSQLToPhysicalPlan(sqlStr, config.timeZone);
    }
    
    public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, DateTimeZone timeZone)
            throws QueryProcessorException, ArgsErrorException, ProcessorException {
        ASTNode astNode = parseSQLToAST(sqlStr);
        Operator operator = parseASTToOperator(astNode, timeZone);
        operator = logicalOptimize(operator, executor);
        PhysicalGenerator physicalGenerator = new PhysicalGenerator(executor);
        return physicalGenerator.transformToPhysicalPlan(operator);
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
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        return ParseUtils.findRootNonNullToken(astTree);
    }

    /**
     * given an unoptimized logical operator tree and return a optimized result.
     *
     * @param operator unoptimized logical operator
     * @param executor
     * @return optimized logical operator
     * @throws LogicalOptimizeException exception in logical optimizing
     */
    private Operator logicalOptimize(Operator operator, QueryProcessExecutor executor) throws LogicalOperatorException {
        switch (operator.getType()) {
            case AUTHOR:
            case METADATA:
            case SET_STORAGE_GROUP:
            case DELETE_TIMESERIES:
            case PROPERTY:
            case LOADDATA:
            case INSERT:
            case INDEX:
            case INDEXQUERY:
                return operator;
            case QUERY:
            case UPDATE:
            case DELETE:
                SFWOperator root = (SFWOperator) operator;
                return optimizeSFWOperator(root, executor);
            default:
                throw new LogicalOperatorException("unknown operator type:" + operator.getType());
        }
    }

    /**
     * given an unoptimized select-from-where operator and return an optimized result.
     *
     * @param root unoptimized select-from-where operator
     * @param executor
     * @return optimized select-from-where operator
     * @throws LogicalOptimizeException exception in SFW optimizing
     */
    private SFWOperator optimizeSFWOperator(SFWOperator root, QueryProcessExecutor executor) throws LogicalOperatorException {
        ConcatPathOptimizer concatPathOptimizer = new ConcatPathOptimizer(executor);
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
