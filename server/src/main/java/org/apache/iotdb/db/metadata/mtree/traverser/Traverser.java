package org.apache.iotdb.db.metadata.mtree.traverser;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_ONE_LEVEL_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_MULTI_LEVEL_WILDCARD;


public abstract class Traverser {

    protected IMNode startNode;
    protected String[] nodes;

    // if isMeasurementTraverser, measurement in template should be processed
    protected boolean isMeasurementTraverser = false;

    // default false means fullPath pattern match
    protected boolean isPrefixMatch = false;

    // level query option
    protected boolean isLevelTraverser = false;
    protected int targetLevel;

    // traverse for specific storage group
    StorageGroupFilter storageGroupFilter = null;

    public Traverser(IMNode startNode, String[] nodes) {
        this.startNode = startNode;
        this.nodes = nodes;
    }

    public void traverse() throws MetadataException {
        traverse(startNode, 1, false, 0);
    }

    protected void traverse(IMNode node, int idx, boolean multiLevelWildcard, int level) throws MetadataException {

        if (storageGroupFilter != null
                && node.isStorageGroup()
                && !storageGroupFilter.satisfy(node.getFullPath())) {
            return;
        }

        if (isLevelTraverser && level > targetLevel) {
            return;
        }

        if (idx >= nodes.length) {
            if (isValid(node)) {
                if (isLevelTraverser) {
                    if (targetLevel == level) {
                        processValidNode(node, idx);
                        return;
                    }
                } else {
                    processValidNode(node, idx);
                }
            }

            if (!multiLevelWildcard && !isPrefixMatch) {
                return;
            }

            processMultiLevelWildcard(node, idx, level);

            return;
        }

        if (isValid(node) || node.isMeasurement()) {
            if (processInternalValid(node, idx) || node.isMeasurement()) {
                return;
            }
        }

        String nodeName = nodes[idx];
        if (PATH_MULTI_LEVEL_WILDCARD.equals(nodeName)) {
            processMultiLevelWildcard(node, idx, level);
        } else if (nodeName.contains(PATH_ONE_LEVEL_WILDCARD)) {
            processOneLevelWildcard(node, idx, multiLevelWildcard, level);
        } else {
            processNameMatch(node, idx, multiLevelWildcard, level);
        }
    }

    protected abstract boolean isValid(IMNode node);

    protected abstract void processValidNode(IMNode node, int idx) throws MetadataException;

    protected abstract boolean processInternalValid(IMNode node, int idx) throws MetadataException;

    protected void processMultiLevelWildcard(IMNode node, int idx, int level) throws MetadataException {
        for (IMNode child : node.getChildren().values()) {
            traverse(child, idx + 1, true, level + 1);
        }

        if (!isMeasurementTraverser || !node.isUseTemplate()) {
            return;
        }

        Template upperTemplate = node.getUpperTemplate();
        for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
            traverse(new MeasurementMNode(node, schema.getMeasurementId(), schema, null), idx + 1, true, level + 1);
        }
    }

    protected void processOneLevelWildcard(IMNode node, int idx, boolean multiLevelWildcard, int level) throws MetadataException {
        String regex = nodes[idx].replace("*", ".*");
        for (IMNode child : node.getChildren().values()) {
            if (!Pattern.matches(regex, child.getName())) {
                continue;
            }
            traverse(child, idx + 1, false, level + 1);
        }
        if (multiLevelWildcard) {
            for (IMNode child : node.getChildren().values()) {
                traverse(child, idx, true, level + 1);
            }
        }

        if (!isMeasurementTraverser || !node.isUseTemplate()) {
            return;
        }

        Template upperTemplate = node.getUpperTemplate();
        for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
            if (!Pattern.matches(regex, schema.getMeasurementId())) {
                continue;
            }
            traverse(new MeasurementMNode(node, schema.getMeasurementId(), schema, null), idx + 1, false, level + 1);
        }
        if (multiLevelWildcard) {
            for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
                traverse(new MeasurementMNode(node, schema.getMeasurementId(), schema, null), idx, true, level + 1);
            }
        }
    }

    protected void processNameMatch(IMNode node, int idx, boolean multiLevelWildcard, int level) throws MetadataException {
        IMNode next = node.getChild(nodes[idx]);
        if (next != null) {
            traverse(next, idx + 1, false, level + 1);
        }
        if (multiLevelWildcard) {
            for (IMNode child : node.getChildren().values()) {
                traverse(child, idx, true, level + 1);
            }
        }

        if (!isMeasurementTraverser || !node.isUseTemplate()) {
            return;
        }

        Template upperTemplate = node.getUpperTemplate();
        IMeasurementSchema targetSchema = upperTemplate.getSchemaMap().get(nodes[idx]);
        if (targetSchema != null) {
            traverse(new MeasurementMNode(node, targetSchema.getMeasurementId(), targetSchema, null), idx + 1, false, level + 1);
        }

        if (multiLevelWildcard) {
            for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
                traverse(new MeasurementMNode(node, schema.getMeasurementId(), schema, null), idx, true, level + 1);
            }
        }
    }

    public void setTargetLevel(int targetLevel) {
        this.targetLevel = targetLevel;
        if (targetLevel > 0) {
            isLevelTraverser = true;
        }
    }

    public void setStorageGroupFilter(StorageGroupFilter storageGroupFilter) {
        this.storageGroupFilter = storageGroupFilter;
    }

    public void setPrefixMatch(boolean isPrefixMatch){
        this.isPrefixMatch = isPrefixMatch;
    }
}
