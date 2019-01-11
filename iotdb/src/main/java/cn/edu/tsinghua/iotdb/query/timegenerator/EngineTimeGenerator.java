package cn.edu.tsinghua.iotdb.query.timegenerator;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.TimeGenerator;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.Node;

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
