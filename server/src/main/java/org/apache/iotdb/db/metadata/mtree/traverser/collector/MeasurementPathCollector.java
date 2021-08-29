package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.regex.Pattern;


public class MeasurementPathCollector extends MeasurementCollector<List<PartialPath>>{

    public MeasurementPathCollector(IMNode startNode, String[] nodes, List<PartialPath> resultSet) {
        super(startNode, nodes, resultSet);
    }

    public MeasurementPathCollector(IMNode startNode, String[] nodes, List<PartialPath> resultSet, int limit, int offset) {
        super(startNode, nodes, resultSet, limit, offset);
    }

    @Override
    protected void collectMeasurementSchema(IMeasurementMNode node) throws MetadataException {
        if (hasLimit) {
            curOffset += 1;
            if (curOffset < offset) {
                return;
            }
        }
        PartialPath path = node.getPartialPath();
        if(nodes[nodes.length - 1].equals(node.getAlias())){
            path.setMeasurementAlias(node.getAlias());
        }
        resultSet.add(path);
        if (hasLimit) {
            count += 1;
        }
    }

    @Override
    protected void collectVectorMeasurementSchema(IMeasurementMNode node, String reg) throws MetadataException {
        IMeasurementSchema schema = node.getSchema();
        List<String> measurements = schema.getValueMeasurementIdList();
        for (String measurement : measurements) {
            if (!Pattern.matches(reg.replace("*", ".*"), measurement)) {
                continue;
            }
            if (hasLimit) {
                curOffset += 1;
                if (curOffset < offset) {
                    return;
                }
            }
            resultSet.add(node.getPartialPath().concatNode(measurement));
            if (hasLimit) {
                count += 1;
            }
        }
    }
}
