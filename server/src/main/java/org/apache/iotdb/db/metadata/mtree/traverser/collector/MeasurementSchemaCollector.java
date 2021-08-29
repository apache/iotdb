package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.metadata.mtree.MTree.getLastTimeStamp;

public class MeasurementSchemaCollector extends MeasurementCollector<List<Pair<PartialPath, String[]>>> {

    QueryContext queryContext;

    public MeasurementSchemaCollector(IMNode startNode, String[] nodes, List<Pair<PartialPath, String[]>> resultSet) {
        super(startNode, nodes, resultSet);
    }

    public MeasurementSchemaCollector(IMNode startNode, String[] nodes, List<Pair<PartialPath, String[]>> resultSet, int limit, int offset) {
        super(startNode, nodes, resultSet, limit, offset);
    }

    public void setQueryContext(QueryContext queryContext) {
        this.queryContext = queryContext;
    }

    @Override
    protected void collectMeasurementSchema(IMeasurementMNode node) throws MetadataException {
        if (hasLimit) {
            curOffset += 1;
            if (curOffset < offset) {
                return;
            }
        }
        IMeasurementSchema measurementSchema = node.getSchema();
        String[] tsRow = new String[7];
        tsRow[0] = node.getAlias();
        tsRow[1] = getStorageGroupPath(node).getFullPath();
        tsRow[2] = measurementSchema.getType().toString();
        tsRow[3] = measurementSchema.getEncodingType().toString();
        tsRow[4] = measurementSchema.getCompressor().toString();
        tsRow[5] = String.valueOf(node.getOffset());
        tsRow[6] =
                needLast
                        ? String.valueOf(getLastTimeStamp(node, queryContext))
                        : null;
        Pair<PartialPath, String[]> temp = new Pair<>(node.getPartialPath(), tsRow);
        resultSet.add(temp);
        if (hasLimit) {
            count += 1;
        }
    }

    @Override
    protected void collectVectorMeasurementSchema(IMeasurementMNode node, String reg) throws MetadataException {
        IMeasurementSchema schema = node.getSchema();
        List<String> measurements = schema.getValueMeasurementIdList();
        for (int i = 0; i < measurements.size(); i++) {
            if (!Pattern.matches(reg.replace("*", ".*"), measurements.get(i))) {
                continue;
            }
            if (hasLimit) {
                curOffset += 1;
                if (curOffset < offset) {
                    return;
                }
            }
            String[] tsRow = new String[7];
            tsRow[0] = null;
            tsRow[1] = getStorageGroupPath(node).getFullPath();
            tsRow[2] = schema.getValueTSDataTypeList().get(i).toString();
            tsRow[3] = schema.getValueTSEncodingList().get(i).toString();
            tsRow[4] = schema.getCompressor().toString();
            tsRow[5] = "-1";
            tsRow[6] =
                    needLast
                            ? String.valueOf(getLastTimeStamp(node, queryContext))
                            : null;
            Pair<PartialPath, String[]> temp =
                    new Pair<>(node.getPartialPath().concatNode(measurements.get(i)), tsRow);
            resultSet.add(temp);
            if (hasLimit) {
                count += 1;
            }
        }
    }

    private PartialPath getStorageGroupPath(IMeasurementMNode node) throws StorageGroupNotSetException {
        if (node == null) {
            return null;
        }
        IMNode temp = node;
        while (temp != null) {
            if (temp.isStorageGroup()) {
                break;
            }
            temp = temp.getParent();
        }
        if (temp == null) {
            throw new StorageGroupNotSetException(node.getFullPath());
        }
        return temp.getPartialPath();
    }
}
