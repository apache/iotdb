package org.apache.iotdb.cluster.query;

import java.util.List;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;

public class ClusterConcatPathOptimizer extends ConcatPathOptimizer {

  private MetaGroupMember metaGroupMember;

  public ClusterConcatPathOptimizer(
      MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected List<String> removeWildcard(String path) throws MetadataException {
    return metaGroupMember.getMatchedPaths(path);
  }


}
