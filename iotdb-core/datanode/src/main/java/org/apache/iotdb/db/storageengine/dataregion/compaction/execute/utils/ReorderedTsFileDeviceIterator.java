package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class ReorderedTsFileDeviceIterator extends TransformedTsFileDeviceIterator {

  private final List<Pair<Pair<IDeviceID, Boolean>, MetadataIndexNode>>
      deviceIDAndFirstMeasurementNodeList = new ArrayList<>();
  private Iterator<Pair<Pair<IDeviceID, Boolean>, MetadataIndexNode>> deviceIDListIterator;
  private Pair<Pair<IDeviceID, Boolean>, MetadataIndexNode> current;

  public ReorderedTsFileDeviceIterator(
      TsFileSequenceReader reader, Function<IDeviceID, IDeviceID> transformer) throws IOException {
    super(reader, transformer);
    collectAndSort();
  }

  public ReorderedTsFileDeviceIterator(
      TsFileSequenceReader reader, String tableName, Function<IDeviceID, IDeviceID> transformer)
      throws IOException {
    super(reader, tableName, transformer);
    collectAndSort();
  }

  private void collectAndSort() throws IOException {
    while (super.hasNext()) {
      Pair<IDeviceID, Boolean> next = super.next();
      deviceIDAndFirstMeasurementNodeList.add(
          new Pair<>(next, super.getFirstMeasurementNodeOfCurrentDevice()));
    }
    deviceIDAndFirstMeasurementNodeList.sort(Comparator.comparing(p -> p.getLeft().getLeft()));
    deviceIDListIterator = deviceIDAndFirstMeasurementNodeList.iterator();
  }

  @Override
  public boolean hasNext() {
    return deviceIDListIterator.hasNext();
  }

  @Override
  public Pair<IDeviceID, Boolean> next() {
    Pair<Pair<IDeviceID, Boolean>, MetadataIndexNode> next = deviceIDListIterator.next();
    current = next;
    return next.left;
  }

  @Override
  public Pair<IDeviceID, Boolean> current() {
    return current == null ? null : current.left;
  }

  @Override
  public MetadataIndexNode getFirstMeasurementNodeOfCurrentDevice() {
    // the devices have been reordered, cannot use the measurementNode
    return current == null ? null : current.right;
  }
}
