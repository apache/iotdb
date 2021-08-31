package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.List;
import java.util.regex.Pattern;


public abstract class MeasurementCollector<T> extends CollectorTraverser<T> {

    public MeasurementCollector(IMNode startNode, String[] nodes, T resultSet) {
        super(startNode, nodes, resultSet);
        isMeasurementTraverser = true;
    }

    public MeasurementCollector(IMNode startNode, String[] nodes, T resultSet, int limit, int offset) {
        super(startNode, nodes, resultSet, limit, offset);
        isMeasurementTraverser = true;
    }

    @Override
    protected boolean isValid(IMNode node) {
        return node.isMeasurement();
    }

    @Override
    public void processValidNode(IMNode node, int idx) throws MetadataException {
        IMeasurementSchema schema = ((IMeasurementMNode) node).getSchema();
        if (schema instanceof MeasurementSchema) {
            if (hasLimit) {
                curOffset += 1;
                if (curOffset < offset) {
                    return;
                }
            }
            collectMeasurementSchema((IMeasurementMNode) node);
            if (hasLimit) {
                count += 1;
            }
        } else if (schema instanceof VectorMeasurementSchema) {
            // only when idx >= nodes.length -1
            List<String> measurements = schema.getValueMeasurementIdList();
            String regex = (idx < nodes.length ? nodes[idx] : "*").replace("*", ".*");
            for (int i = 0; i < measurements.size(); i++) {
                if (!Pattern.matches(regex, measurements.get(i))) {
                    continue;
                }
                if (hasLimit) {
                    curOffset += 1;
                    if (curOffset < offset) {
                        return;
                    }
                }
                collectVectorMeasurementSchema((IMeasurementMNode) node, i);
                if (hasLimit) {
                    count += 1;
                }
            }
        }
    }

    @Override
    protected boolean processInternalValid(IMNode node, int idx) throws MetadataException {
        if (idx == nodes.length - 1) {
            if (((IMeasurementMNode) node).getSchema() instanceof VectorMeasurementSchema) {
                processValidNode(node, idx);
            }
        }
        return true;
    }

    protected abstract void collectMeasurementSchema(IMeasurementMNode node) throws MetadataException;

    protected abstract void collectVectorMeasurementSchema(IMeasurementMNode node, int index) throws MetadataException;

}
