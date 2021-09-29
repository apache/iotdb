package org.apache.iotdb.db.metadata.mtree.traverser.collector;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;

// This class defines MeasurementMNode as target node
// and defines the measurement process framework.
// MultiMeasurement will be processed as one unit.
public abstract class MeasurementCollector<T> extends CollectorTraverser<T> {

  public MeasurementCollector(IMNode startNode, PartialPath path) throws MetadataException {
    super(startNode, path);
    isMeasurementTraverser = true;
  }

  @Override
  protected boolean processInternalMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException {
    return false;
  }

  @Override
  protected boolean processFullMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException {
    if (!node.isMeasurement()) {
      return false;
    }
    collectMeasurement(node.getAsMeasurementMNode());
    return true;
  }

  /**
   * collect the information of one measurement
   *
   * @param node MeasurementMNode holding the measurement schema
   */
  protected abstract void collectMeasurement(IMeasurementMNode node);
}
