package org.apache.iotdb.db.qp;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.qp.IllegalASTFormatException;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.exception.qp.LogicalOptimizeException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.DNFFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.MergeSingleFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.RemoveNotOptimizer;
import org.apache.iotdb.db.sql.ParseGenerator;
import org.apache.iotdb.db.sql.parse.ASTNode;
import org.apache.iotdb.db.sql.parse.ParseException;
import org.apache.iotdb.db.sql.parse.ParseUtils;

import java.time.ZoneId;

/**
 * provide a integration method for other user.
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
        IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
        return parseSQLToPhysicalPlan(sqlStr, config.getZoneID());
    }

    public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, ZoneId zoneId)
            throws QueryProcessorException, ArgsErrorException, ProcessorException {
        ASTNode astNode = parseSQLToAST(sqlStr);
        Operator operator = parseASTToOperator(astNode, zoneId);
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
    private RootOperator parseASTToOperator(ASTNode astNode, ZoneId zoneId) throws QueryProcessorException, ArgsErrorException {
        LogicalGenerator generator = new LogicalGenerator(zoneId);
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
        if (filter == null) {
            return root;
        }
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
