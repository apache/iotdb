package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;

public interface Frame {
  Range getRange(int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd);
}
