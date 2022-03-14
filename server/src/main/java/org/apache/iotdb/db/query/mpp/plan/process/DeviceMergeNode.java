package org.apache.iotdb.db.query.mpp.plan.process;

import org.apache.iotdb.db.query.mpp.common.OrderBy;
import org.apache.iotdb.db.query.mpp.common.TsBlock;
import org.apache.iotdb.db.query.mpp.common.WithoutPolicy;
import org.apache.iotdb.db.query.mpp.plan.PlanNode;
import org.apache.iotdb.db.query.mpp.plan.PlanNodeId;

import java.util.Map;

/**
 * DeviceMergeOperator is responsible for constructing a device-based view of a set of series. And output the result with
 * specific order. The order could be 'order by device' or 'order by timestamp'
 *
 * Each output from its children should have the same schema. That means, the columns should be same between these TsBlocks.
 * If the input TsBlock contains n columns, the device-based view will contain n+1 columns where the new column is Device
 * column.
 *
 */
public class DeviceMergeNode extends ProcessNode {
    // The result output order that this operator
    private OrderBy mergeOrder;

    // The policy to decide whether a row should be discarded
    // The without policy is able to be push down to the DeviceMergeNode because we can know whether a row contains
    // null or not.
    private WithoutPolicy withoutPolicy;

    // The map from deviceName to corresponding query result node responsible for that device.
    // DeviceNode means the node whose output TsBlock contains the data belonged to one device.
    private Map<String, PlanNode<TsBlock>> childDeviceNodeMap;

    public DeviceMergeNode(PlanNodeId id) {
        super(id);
    }

    public DeviceMergeNode(PlanNodeId id, Map<String, PlanNode<TsBlock>> deviceNodeMap) {
        this(id);
        this.childDeviceNodeMap = deviceNodeMap;
        this.children.addAll(deviceNodeMap.values());
    }

    public void addChildDeviceNode(String deviceName, PlanNode<TsBlock> childNode) {
        this.childDeviceNodeMap.put(deviceName, childNode);
        this.children.add(childNode);
    }
}
