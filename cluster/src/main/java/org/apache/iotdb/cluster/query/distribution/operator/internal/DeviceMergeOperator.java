package org.apache.iotdb.cluster.query.distribution.operator;

import org.apache.iotdb.cluster.query.distribution.common.Tablet;
import org.apache.iotdb.cluster.query.distribution.common.TraversalOrder;

import java.util.List;
import java.util.Map;

/**
 * DeviceMergeOperator is responsible for constructing a device-based view of a set of series. And
 * output the result with specific order. The order could be 'order by device' or 'order by
 * timestamp'
 *
 * <p>The types of involved devices should be same. If the device contains n series, the
 * device-based view will contain n+2 columns, which are timestamp column, device name column and n
 * value columns of involved series.
 *
 * <p>Children type: [TimeJoinOperator]
 */
public class DeviceMergeOperator extends InternalOperator<Tablet> {
  // The result output order that this operator
  private TraversalOrder mergeOrder;

  // Owned devices
  private List<String> ownedDeviceNameList;

  // The map from deviceName to corresponding query result operator responsible for that device.
  private Map<String, TimeJoinOperator> upstreamMap;

  @Override
  public boolean hasNext() {
    return false;
  }

  // If the Tablet from TimeJoinOperator has n columns, the output of DeviceMergeOperator will
  // contain n+1 columns where
  // the additional column is `deviceName`
  // And, the `alignedByDevice` in the TabletMetadata will be `true`
  @Override
  public Tablet getNextBatch() {
    return null;
  }
}
