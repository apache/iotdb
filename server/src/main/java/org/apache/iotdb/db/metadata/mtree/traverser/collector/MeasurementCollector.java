package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.regex.Pattern;

public abstract class MeasurementCollector<T> extends CollectorTraverser {

    protected T resultSet;

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

    @Override
    protected void processMultiLevelWildcard(IMNode node, int idx, boolean multiLevelWildcard) throws MetadataException {

        super.processMultiLevelWildcard(node, idx, multiLevelWildcard);

        if (node.isUseTemplate()) {
            Template upperTemplate = node.getUpperTemplate();
            for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
                traverse(new MeasurementMNode(node, schema.getMeasurementId(), schema, null), idx + 1, true);
            }
        }
    }

    @Override
    protected void processOneLevelWildcard(IMNode node, int idx, boolean multiLevelWildcard) throws MetadataException {

        super.processOneLevelWildcard(node, idx, multiLevelWildcard);

        String regex = nodes[idx].replace("*", ".*");
        if (node.isUseTemplate()) {
            for (IMeasurementSchema schema : node.getUpperTemplate().getSchemaMap().values()) {
                if (!Pattern.matches(regex, schema.getMeasurementId())) {
                    continue;
                }
                traverse(new MeasurementMNode(node, schema.getMeasurementId(), schema, null), idx + 1, false);
            }
            if (multiLevelWildcard) {
                for (IMeasurementSchema schema : node.getUpperTemplate().getSchemaMap().values()) {
                    traverse(new MeasurementMNode(node, schema.getMeasurementId(), schema, null), idx, true);
                }
            }
        }
    }

    @Override
    protected void processNameMatch(IMNode node, int idx, boolean multiLevelWildcard) throws MetadataException {

        super.processNameMatch(node, idx, multiLevelWildcard);

        if (node.isUseTemplate()) {
            Template upperTemplate = node.getUpperTemplate();
            IMeasurementSchema targetSchema = upperTemplate.getSchemaMap().get(nodes[idx]);
            if (targetSchema != null) {
                traverse(new MeasurementMNode(node, targetSchema.getMeasurementId(), targetSchema, null), idx + 1, false);
            }

            if (multiLevelWildcard) {
                for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
                    traverse(new MeasurementMNode(node, schema.getMeasurementId(), targetSchema, null), idx, true);
                }
            }
        }
    }

    protected abstract void collectMeasurementSchema(IMeasurementMNode node) throws MetadataException;

    protected abstract void collectVectorMeasurementSchema(IMeasurementMNode node, String reg) throws MetadataException;

}
