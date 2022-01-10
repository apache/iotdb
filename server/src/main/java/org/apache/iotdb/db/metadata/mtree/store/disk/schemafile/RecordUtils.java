package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;

import java.nio.ByteBuffer;

/**
 * This class translate an IMNode into a bytebuffer, or otherwise.
 * When definition of IMNode changes, this class changes as well.
 */
public class RecordUtils {

  /**
   * Internal MNode Record Structure (in bytes):
   * 1 byte: nodeType, 0 for internal, 1 for measurement
   * 1 short (2 byte): recLength, length of whole record
   *
   * @param node
   * @return
   */
  public static ByteBuffer node2Buffer(InternalMNode node) {
    return null;
  }

  public static ByteBuffer node2Buffer(MeasurementMNode node) {
    return null;
  }

  public static IMNode buffer2Node(ByteBuffer buffer) {
    return null;
  }

}
