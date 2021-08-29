package org.apache.iotdb.db.metadata.mtree.traverser;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.regex.Pattern;


public abstract class Traverser {

    protected static final String PATH_MULTI_LEVEL_WILDCARD = "**";
    protected static final String PATH_ONE_LEVEL_WILDCARD = "*";

    protected IMNode startNode;
    protected String[] nodes;

    protected boolean isMeasurementTraverser = false;

    public Traverser(IMNode startNode, String[] nodes){
        this.startNode = startNode;
        this.nodes = nodes;
    }

    public void traverse() throws MetadataException {
        traverse(startNode, 1, false);
    }

    protected void traverse(IMNode node, int idx, boolean multiLevelWildcard) throws MetadataException {
        if (idx >= nodes.length) {
            if (isValid(node)) {
                processValidNode(node, idx);
            }

            if (!multiLevelWildcard) {
                return;
            }

            processMultiLevelWildcard(node, idx, true);

            return;
        }

        if (isValid(node) || node.isMeasurement()) {
            if (processInternalValid(node, idx) || node.isMeasurement()) {
                return;
            }
        }

        String nodeName = nodes[idx];
        if (PATH_MULTI_LEVEL_WILDCARD.equals(nodeName)) {
            processMultiLevelWildcard(node, idx, multiLevelWildcard);
        } else if (nodeName.contains(PATH_ONE_LEVEL_WILDCARD)) {
            processOneLevelWildcard(node, idx, multiLevelWildcard);
        } else {
            processNameMatch(node, idx, multiLevelWildcard);
        }
    }

    protected abstract boolean isValid(IMNode node);

    protected abstract void processValidNode(IMNode node, int idx) throws MetadataException;

    protected abstract boolean processInternalValid(IMNode node, int idx) throws MetadataException;

    protected void processMultiLevelWildcard(IMNode node, int idx, boolean multiLevelWildcard) throws MetadataException {
        for (IMNode child : node.getChildren().values()) {
            traverse(child, idx + 1, true);
        }

        if (!isMeasurementTraverser) {
            return;
        }

        if (node.isUseTemplate()) {
            Template upperTemplate = node.getUpperTemplate();
            for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
                traverse(new MeasurementMNode(node, schema.getMeasurementId(), schema, null), idx + 1, true);
            }
        }
    }

    protected void processOneLevelWildcard(IMNode node, int idx, boolean multiLevelWildcard) throws MetadataException {
        String regex = nodes[idx].replace("*", ".*");
        for (IMNode child : node.getChildren().values()) {
            if (!Pattern.matches(regex, child.getName())) {
                continue;
            }
            traverse(child, idx + 1, false);
        }
        if (multiLevelWildcard) {
            for (IMNode child : node.getChildren().values()) {
                traverse(child, idx, true);
            }
        }

        if (!isMeasurementTraverser) {
            return;
        }

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

    protected void processNameMatch(IMNode node, int idx, boolean multiLevelWildcard) throws MetadataException {
        IMNode next = node.getChild(nodes[idx]);
        if (next != null) {
            traverse(next, idx + 1, false);
        }
        if (multiLevelWildcard) {
            for (IMNode child : node.getChildren().values()) {
                traverse(child, idx, true);
            }
        }

        if (!isMeasurementTraverser) {
            return;
        }

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
}
