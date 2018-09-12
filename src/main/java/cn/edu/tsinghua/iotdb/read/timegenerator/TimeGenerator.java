package cn.edu.tsinghua.iotdb.read.timegenerator;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.Node;

import java.io.IOException;

/**
 * A timestamp generator for query with filter.
 * e.g. For query clause "select s1, s2 form root where s3 < 0 and time > 100"，
 * this class can iterate back to every timestamp of the query.
 */
public class TimeGenerator implements TimestampGenerator {

    private QueryFilter queryFilter;
    private Node operatorNode;

    public TimeGenerator(QueryFilter queryFilter) throws IOException, FileNodeManagerException {
        this.queryFilter = queryFilter;
        initNode();
    }

    private void initNode() throws IOException, FileNodeManagerException {
        NodeConstructor nodeConstructor = new NodeConstructor();
        this.operatorNode = nodeConstructor.construct(queryFilter);
    }

    @Override
    public boolean hasNext() throws IOException {
        return operatorNode.hasNext();
    }

    @Override
    public long next() throws IOException {
        return operatorNode.next();
    }
}
