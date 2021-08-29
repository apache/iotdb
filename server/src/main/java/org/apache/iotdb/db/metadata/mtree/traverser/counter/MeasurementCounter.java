package org.apache.iotdb.db.metadata.mtree.traverser.counter;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.List;
import java.util.regex.Pattern;


public class MeasurementCounter extends CounterTraverser {

    public MeasurementCounter() {
        isMeasurementTraverser = true;
    }

    @Override
    protected boolean isValid(IMNode node) {
        return node.isMeasurement();
    }

    @Override
    protected void processValidNode(IMNode node, int idx) throws MetadataException {
        IMeasurementSchema schema = ((IMeasurementMNode) node).getSchema();
        if (schema instanceof MeasurementSchema) {
            count++;
        } else if (schema instanceof VectorMeasurementSchema) {
            // only when idx >= nodes.length -1
            List<String> measurements = schema.getValueMeasurementIdList();
            if (idx >= nodes.length) {
                count += ((IMeasurementMNode) node).getMeasurementCount();
            } else {
                String regex = nodes[idx].replace("*", ".*");
                for (String measurement : measurements) {
                    if (Pattern.matches(regex, measurement)) {
                        count++;
                    }
                }
            }
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
}
