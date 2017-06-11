package cn.edu.thu.tsfiledb.sql.exec;

import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.qp.exception.ErrorQueryOpException;
import cn.edu.thu.tsfiledb.qp.exception.IllegalASTFormatException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.optimize.LogicalOptimizeException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.RootOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter.FilterOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw.SFWOperator;
import cn.edu.thu.tsfiledb.qp.logical.optimizer.ConcatPathOptimizer;
import cn.edu.thu.tsfiledb.qp.logical.optimizer.filter.DNFFilterOptimizer;
import cn.edu.thu.tsfiledb.qp.logical.optimizer.filter.MergeSingleFilterOptimizer;
import cn.edu.thu.tsfiledb.qp.logical.optimizer.filter.RemoveNotOptimizer;
import cn.edu.thu.tsfiledb.qp.physical.optimizer.IPhysicalOptimizer;
import cn.edu.thu.tsfiledb.qp.physical.optimizer.NonePhysicalOptimizer;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.ParseException;
import cn.edu.thu.tsfiledb.sql.parse.ParseUtils;

/**
 * provide a integration method for other user.
 * 
 * @author kangrong
 *
 */
public class TSqlParserV2 {
    Logger LOG = LoggerFactory.getLogger(TSqlParserV2.class);

    public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, QueryProcessExecutor executor)
            throws QueryProcessorException {
        ASTNode astTree;
        // parse string to ASTTree
        try {
            astTree = ParseGenerator.generateAST(sqlStr);
        } catch (ParseException e) {
            throw new IllegalASTFormatException("parsing error,message:" + e.getMessage());
        }
        astTree = ParseUtils.findRootNonNullToken(astTree);
        TSPlanContextV2 tsPlan = new TSPlanContextV2();
        tsPlan.analyze(astTree);
        Operator operator = tsPlan.getOperator();
        if (operator.isQuery()) {
            SFWOperator root = (SFWOperator) operator;
            root = logicalOptimize(root);
            PhysicalPlan plan = transformToPhysicalPlan(root, executor);
            plan = executor.queryPhysicalOptimize(plan);
            return plan;
        } else {
            PhysicalPlan plan;
            switch (operator.getType()) {
                case AUTHOR:
                case METADATA:
                case PROPERTY:
                case LOADDATA:
                    // for INSERT/UPDATE/DELETE, it needn't logical optimization and physical
                    // optimization
                    RootOperator author = (RootOperator) operator;
                    plan = transformToPhysicalPlan(author, executor);
//                    System.out.println("com.corp.tsfile.sql.exec.TSqlParserV2:\n"
//                            + plan.printQueryPlan());
                    return plan;
                case MULTIINSERT:
                	SFWOperator multInsertOp = (SFWOperator) operator;
                    plan = transformToPhysicalPlan(multInsertOp, executor);
                    plan = executor.nonQueryPhysicalOptimize(plan);
                    return plan;
                case UPDATE:
                case INSERT:
                case DELETE:
                    SFWOperator root = (SFWOperator) operator;
                    root = logicalOptimize(root);
                    plan = transformToPhysicalPlan(root, executor);
//                    System.out.println("com.corp.tsfile.sql.exec.TSqlParserV2:\n"
//                            + plan.printQueryPlan());
                    plan = executor.nonQueryPhysicalOptimize(plan);
                    return plan;

                default:
                    throw new ErrorQueryOpException("unknown operator type:{}" + operator.getType());
            }
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
    public RootOperator parseSQLToOperator(String sqlStr) throws QueryProcessorException {
        ASTNode astTree;
        // parse string to ASTTree
        try {
            astTree = ParseGenerator.generateAST(sqlStr);
        } catch (ParseException e) {
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr+ " .message:" + e.getMessage());
        }
        astTree = ParseUtils.findRootNonNullToken(astTree);
        TSPlanContextV2 tsPlan = new TSPlanContextV2();
        tsPlan.analyze(astTree);
        return tsPlan.getOperator();
    }

    /**
     * given an unoptimized logical operator tree and return a optimized result.
     * 
     * @param root
     * @return
     * @throws LogicalOptimizeException
     */
    public SFWOperator logicalOptimize(SFWOperator root) throws LogicalOptimizeException {
        ConcatPathOptimizer concatPathOptimizer = new ConcatPathOptimizer();
        root = (SFWOperator) concatPathOptimizer.transform(root);
        FilterOperator filter = root.getFilterOperator();
        // this means no filter, in insert or query
        if (filter == null)
            return root;
        // System.out.println(filter.showTree());
        RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
        filter = removeNot.optimize(filter);
        // System.out.println(filter.showTree());
        DNFFilterOptimizer dnf = new DNFFilterOptimizer();
        filter = dnf.optimize(filter);
        MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
        filter = merge.optimize(filter);
        root.setFilterOperator(filter);
        // System.out.println(filter.showTree());
        return root;
    }

    /**
     * transform a root operator to a physical plan
     * 
     * @param root
     * @param conf
     * @return
     * @throws QueryProcessorException
     */
    public PhysicalPlan transformToPhysicalPlan(RootOperator root, QueryProcessExecutor conf)
            throws QueryProcessorException {
        return root.transformToPhysicalPlan(conf);
    }

    /**
     * given an unoptimized physical plan and return a optimized result.Up to now, physical
     * optimizer do nothing.
     * 
     * @since 2016-10-11
     * @param physicalPlan
     * @return
     */
    public PhysicalPlan physicalOptimize(PhysicalPlan physicalPlan, QueryProcessExecutor conf) {

            IPhysicalOptimizer physicalOptimizer = new NonePhysicalOptimizer();

            return physicalOptimizer.transform(physicalPlan, conf);
    }

    /**
     * for a query statement, input an SQL statement and return a iterator of {@code QueryDataSet}.
     * 
     * @param op
     * @param conf
     * @return - if parameter op is not in type of QUERY, throw exception. Otherwise, return an
     *         {@linkplain java.util.Iterator Iterator}
     * @throws QueryProcessorException
     */
    public Iterator<QueryDataSet> query(RootOperator op, QueryProcessExecutor conf)
            throws QueryProcessorException {
        if (op.isQuery()) {
            SFWOperator root = (SFWOperator) op;
            root = logicalOptimize(root);
            PhysicalPlan plan = transformToPhysicalPlan(root, conf);
            plan = conf.queryPhysicalOptimize(plan);
            return plan.processQuery(conf);
        } else {
            throw new ErrorQueryOpException("cannot execute query for a non-query operator:{}"
                    + op.getType());
        }
    }

    /**
     * for a non-query statement(insert/update/delete), input an SQL statement and return whether
     * this operator has been successfully.
     * 
     * @param op
     * @param conf
     * @return
     * @throws QueryProcessorException
     * @throws ProcessorException 
     */
    public boolean nonQuery(RootOperator op, QueryProcessExecutor conf)
            throws QueryProcessorException, ProcessorException {
        PhysicalPlan plan;
        switch (op.getType()) {
            case AUTHOR:
            case METADATA:
            case PROPERTY:
            case LOADDATA:
                // for INSERT/UPDATE/DELETE, it needn't logical optimization and physical
                // optimization
                RootOperator author = (RootOperator) op;
                plan = transformToPhysicalPlan(author, conf);
//                System.out.println("com.corp.tsfile.sql.exec.TSqlParserV2:\n"
//                        + plan.printQueryPlan());
                return plan.processNonQuery(conf);
            case MULTIINSERT:
            	 SFWOperator multInsertOp = (SFWOperator) op;
                 plan = transformToPhysicalPlan(multInsertOp, conf);
                 plan = conf.nonQueryPhysicalOptimize(plan);
                 return plan.processNonQuery(conf);
            case UPDATE:
            case INSERT:
            case DELETE:
                SFWOperator root = (SFWOperator) op;
                root = logicalOptimize(root);
                plan = transformToPhysicalPlan(root, conf);
//                System.out.println("com.corp.tsfile.sql.exec.TSqlParserV2:\n"
//                        + plan.printQueryPlan());
                plan = conf.nonQueryPhysicalOptimize(plan);
                return plan.processNonQuery(conf);

            default:
                throw new ErrorQueryOpException("unknown operator type:{}" + op.getType());
        }
    }
}
