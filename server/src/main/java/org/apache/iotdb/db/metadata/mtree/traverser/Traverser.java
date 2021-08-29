package org.apache.iotdb.db.metadata.mtree.traverser;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.regex.Pattern;


public abstract class Traverser {

    protected static final String PATH_MULTI_LEVEL_WILDCARD = "**";
    protected static final String PATH_ONE_LEVEL_WILDCARD = "*";

    protected String[] nodes;

    public void setNodes(String[] nodes) {
        this.nodes = nodes;
    }

    public void traverse(IMNode startNode) throws MetadataException {
        traverse(startNode, 1, false);
    }

    public void traverse(IMNode node, int idx, boolean multiLevelWildcard) throws MetadataException {
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

        if (isValid(node)) {
            if (processInternalValid(node, idx)) {
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
    }
}
