package cn.edu.tsinghua.iotdb.read.timegenerator;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.Node;

import java.io.IOException;

/**
 * IoTDB level timestamp generator for query with filter.
 * e.g. "select s1, s2 form root where s3 < 0 and time > 100"
 */
public class DeltaTimeGenerator implements TimestampGenerator {

    private QueryFilter queryFilter;
    private Node operatorNode;

    public DeltaTimeGenerator(QueryFilter queryFilter) throws IOException {
        this.queryFilter = queryFilter;
    }

    private void initNode() throws IOException {
        DeltaNodeConstructor deltaNodeConstructor = new DeltaNodeConstructor();
        this.operatorNode = deltaNodeConstructor.construct(queryFilter);
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
