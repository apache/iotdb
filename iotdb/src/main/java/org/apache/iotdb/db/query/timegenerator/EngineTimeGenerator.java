package org.apache.iotdb.db.query.timegenerator;

import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.db.exception.FileNodeManagerException;

import java.io.IOException;

/**
 * <p> A timestamp generator for query with filter.
 * e.g. For query clause "select s1, s2 form root where s3 < 0 and time > 100",
 * this class can iterate back to every timestamp of the query.
 */
public class EngineTimeGenerator implements TimeGenerator {

    private IExpression expression;
    private Node operatorNode;
    private long jobId;

    public EngineTimeGenerator(long jobId, IExpression expression) throws IOException, FileNodeManagerException {
        this.jobId = jobId;
        this.expression = expression;
        initNode();
    }

    private void initNode() throws IOException, FileNodeManagerException {
        EngineNodeConstructor engineNodeConstructor = new EngineNodeConstructor(jobId);
        this.operatorNode = engineNodeConstructor.construct(expression);
    }

    @Override
    public boolean hasNext() throws IOException {
        return operatorNode.hasNext();
    }

    @Override
    public long next() throws IOException {
        return operatorNode.next();
    }

    // TODO implement the optimization
    @Override public Object getValue(Path path, long time) {
        return null;
    }

}
