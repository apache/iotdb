package org.apache.iotdb.cluster.query.filter;

import java.util.List;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlotTsFileFilter implements TsFileFilter {

  private static final Logger logger = LoggerFactory.getLogger(SlotTsFileFilter.class);
  private List<Integer> slots;

  public SlotTsFileFilter(List<Integer> slots) {
    this.slots = slots;
  }

  @Override
  public boolean fileNotSatisfy(TsFileResource resource) {
    return fileNotInSlots(resource, slots);
  }

  public static boolean fileNotInSlots(TsFileResource res, List<Integer> nodeSlots) {
    // {storageGroupName}/{partitionNum}/{fileName}
    String[] pathSegments = PartitionUtils.splitTsFilePath(res);
    String storageGroupName = pathSegments[pathSegments.length - 3];
    int partitionNum = Integer.parseInt(pathSegments[pathSegments.length - 2]);
    int slot = PartitionUtils.calculateStorageGroupSlot(storageGroupName, partitionNum,
        ClusterConstant.SLOT_NUM);
    logger.debug("The slot of {} is {}", res.getFile().getPath(), slot);
    return !nodeSlots.contains(slot);
  }
}
