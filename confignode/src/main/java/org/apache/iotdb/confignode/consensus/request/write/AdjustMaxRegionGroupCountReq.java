package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AdjustMaxRegionGroupCountReq extends ConfigRequest {

  // Map<StorageGroupName, Pair<maxSchemaRegionGroupCount, maxDataRegionGroupCount>>
  public final Map<String, Pair<Integer, Integer>> maxRegionGroupCountMap;

  public AdjustMaxRegionGroupCountReq() {
    super(ConfigRequestType.AdjustMaxRegionGroupCount);
    this.maxRegionGroupCountMap = new HashMap<>();
  }

  public void putEntry(String storageGroup, Pair<Integer, Integer> maxRegionGroupCount) {
    maxRegionGroupCountMap.put(storageGroup, maxRegionGroupCount);
  }

  public Map<String, Pair<Integer, Integer>> getMaxRegionGroupCountMap() {
    return maxRegionGroupCountMap;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(ConfigRequestType.AdjustMaxRegionGroupCount.ordinal(), stream);

    ReadWriteIOUtils.write(maxRegionGroupCountMap.size(), stream);
    for (Map.Entry<String, Pair<Integer, Integer>> maxRegionGroupCountEntry :
        maxRegionGroupCountMap.entrySet()) {
      ReadWriteIOUtils.write(maxRegionGroupCountEntry.getKey(), stream);
      ReadWriteIOUtils.write(maxRegionGroupCountEntry.getValue().getLeft(), stream);
      ReadWriteIOUtils.write(maxRegionGroupCountEntry.getValue().getRight(), stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int storageGroupNum = buffer.getInt();

    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = ReadWriteIOUtils.readString(buffer);
      int maxSchemaRegionGroupCount = buffer.getInt();
      int maxDataRegionGroupCount = buffer.getInt();
      maxRegionGroupCountMap.put(
          storageGroup, new Pair<>(maxSchemaRegionGroupCount, maxDataRegionGroupCount));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AdjustMaxRegionGroupCountReq that = (AdjustMaxRegionGroupCountReq) o;
    return maxRegionGroupCountMap.equals(that.maxRegionGroupCountMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxRegionGroupCountMap);
  }
}
