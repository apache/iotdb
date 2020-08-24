package org.apache.iotdb.cluster;

import java.util.List;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author HouliangQi (neuyilan@163.com)
 * @description
 * @since 2020-08-22 16:47
 */
public class DataGroupMemberFlushPlanPolicy implements TsFileFlushPolicy {

  private static final Logger logger = LoggerFactory
      .getLogger(DataGroupMemberFlushPlanPolicy.class);
  private DataGroupMember dataGroupMember;

  public DataGroupMemberFlushPlanPolicy(DataGroupMember dataGroupMember) {
    this.dataGroupMember = dataGroupMember;
  }

  @Override
  public void apply(StorageGroupProcessor storageGroupProcessor, TsFileProcessor processor,
      boolean isSeq) {
    // do nothing
  }

  @Override
  public void apply(List<Path> storeGroups) {
    FlushPlan plan = new FlushPlan(null, true, storeGroups);
    boolean result = dataGroupMember.flushFileWhenDoSnapshot(plan);
    if (!result) {
      logger.error("flush file when do snapshot failed");
    }
  }
}

