package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_MULTI_LEVEL_WILDCARD;

public class TSEntityPathCollector extends CollectorTraverser<Set<PartialPath>>{

    public TSEntityPathCollector(IMNode startNode, String[] nodes, Set<PartialPath> resultSet) {
        super(startNode, nodes, resultSet);
    }

    @Override
    protected boolean isValid(IMNode node) {
        return node.isMeasurement();
    }

    @Override
    public void processValidNode(IMNode node, int idx) throws MetadataException {
        IMeasurementSchema schema = ((IMeasurementMNode) node).getSchema();
        if (schema instanceof MeasurementSchema) {
            resultSet.add(node.getParent().getPartialPath());
        } else if (schema instanceof VectorMeasurementSchema) {
            if(idx == nodes.length && !nodes[nodes.length-1].equals(PATH_MULTI_LEVEL_WILDCARD)&&!isPrefixMatch){
                return;
            }
            // only when idx > nodes.length or nodes ends with ** or isPrefixMatch
            resultSet.add(node.getParent().getPartialPath());
        }
    }

    @Override
    protected boolean processInternalValid(IMNode node, int idx) throws MetadataException {
        if (idx == nodes.length - 1) {
            IMeasurementSchema schema = ((IMeasurementMNode) node).getSchema();
            if (schema instanceof VectorMeasurementSchema) {
                List<String> measurements = schema.getValueMeasurementIdList();
                String regex = nodes[idx].replace("*", ".*");
                for (String measurement : measurements) {
                    if (!Pattern.matches(regex, measurement)) {
                        resultSet.add(node.getParent().getPartialPath());
                        break;
                    }
                }
            }
        }
        return true;
    }
}
