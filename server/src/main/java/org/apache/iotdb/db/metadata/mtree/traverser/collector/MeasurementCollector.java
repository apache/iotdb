package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;


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
            collectMeasurementSchema((IMeasurementMNode) node);
        } else if (schema instanceof VectorMeasurementSchema) {
            // only when idx >= nodes.length -1
            collectVectorMeasurementSchema((IMeasurementMNode) node, idx < nodes.length ? nodes[idx] : "*");
        }
    }

    @Override
    protected boolean processInternalValid(IMNode node, int idx) throws MetadataException {
        IMeasurementSchema schema = ((IMeasurementMNode) node).getSchema();
        if (schema instanceof VectorMeasurementSchema) {
            if (idx == nodes.length - 1) {
                processValidNode(node, idx);
            }
        }
        return true;
    }

    protected abstract void collectMeasurementSchema(IMeasurementMNode node) throws MetadataException;

    protected abstract void collectVectorMeasurementSchema(IMeasurementMNode node, String reg) throws MetadataException;

}
