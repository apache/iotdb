package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.common.TraversalOrder;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;

import java.util.List;
import java.util.Map;

/**
 * DeviceMergeOperator is responsible for constructing a device-based view of a set of series. And output the result with
 * specific order. The order could be 'order by device' or 'order by timestamp'
 *
 * The types of involved devices should be same. If the device contains n series, the device-based view will contain n+2
 * columns, which are timestamp column, device name column and n value columns of involved series.
 *
 * Children type: [TimeJoinOperator]
 */
public class DeviceMergeNode extends ProcessNode<TsBlock> {
    // The result output order that this operator
    private TraversalOrder mergeOrder;

    // Owned devices
    private List<String> ownedDeviceNameList;

    // The map from deviceName to corresponding query result operator responsible for that device.
    private Map<String, TimeJoinNode> upstreamMap;
}
