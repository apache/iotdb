package org.apache.iotdb.db.index.usable;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The data which has flushed out may be updated due to the "unsequence data" or deletion. As we
 * cannot assume that all index techniques are able to support data deletion or update, the index
 * framework introduces the concept of "index usability range". In the time range which is marked as
 * "index unusable", the correctness of index's pruning phase is not guaranteed.
 *
 * <p>A natural solution is to put the data in the "index unusable" range into the post-processing
 * phase (or called refinement phase) directly.
 *
 * <p>TODO The IIndexUsable's update due to the "merge" finishing hasn't been taken in account.
 */
public interface IIndexUsable {

  /**
   * add a range where index is usable.
   *
   * @param fullPath the path of time series
   * @param start start timestamp
   * @param end end timestamp
   */
  void addUsableRange(PartialPath fullPath, long start, long end);

  /**
   * minus a range where index is usable.
   *
   * @param fullPath the path of time series
   * @param start start timestamp
   * @param end end timestamp
   */
  void minusUsableRange(PartialPath fullPath, long start, long end);

  /**
   * The result format depends on "sub-matching" ({@linkplain SubMatchIndexUsability}) or
   * "whole-matching" ({@linkplain WholeMatchIndexUsability})
   *
   * @return the range where index is unusable.
   */
  Object getUnusableRange();

  void serialize(OutputStream outputStream) throws IOException;

  void deserialize(InputStream inputStream) throws IllegalPathException, IOException;

  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexUsable createEmptyIndexUsability(PartialPath path) {
      if (path.isFullPath()) {
        return new SubMatchIndexUsability();
      } else {
        return new WholeMatchIndexUsability();
      }
    }

    public static IIndexUsable deserializeIndexUsability(PartialPath path, InputStream inputStream)
        throws IOException, IllegalPathException {
      IIndexUsable res;
      if (path.isFullPath()) {
        res = new SubMatchIndexUsability();
        res.deserialize(inputStream);
      } else {
        res = new WholeMatchIndexUsability();
        res.deserialize(inputStream);
      }
      return res;
    }
  }
}
