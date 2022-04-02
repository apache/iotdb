package org.apache.iotdb.commons.partition;

import org.apache.iotdb.consensus.common.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Endpoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RegionReplicaSet {
  private ConsensusGroupId consensusGroupId;

  private List<Endpoint> endPointList;

  public RegionReplicaSet(ConsensusGroupId consensusGroupId, List<Endpoint> endPointList) {
    this.consensusGroupId = consensusGroupId;
    this.endPointList = endPointList;
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public void setConsensusGroupId(ConsensusGroupId consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
  }

  public List<Endpoint> getEndPointList() {
    return endPointList;
  }

  public void setEndPointList(List<Endpoint> endPointList) {
    this.endPointList = endPointList;
  }

  public String toString() {
    return String.format("%s:%s", consensusGroupId, endPointList);
  }

  public int hashCode() {
    return toString().hashCode();
  }

  public boolean equals(Object obj) {
    return obj instanceof RegionReplicaSet && obj.toString().equals(toString());
  }

  public void serialize(ByteBuffer byteBuffer) {
    consensusGroupId.serialize(byteBuffer);
    byteBuffer.putInt(endPointList.size());
    endPointList.forEach(
        endpoint -> {
          endpoint.serializeImpl(byteBuffer);
        });
  }

  public static RegionReplicaSet deserialize(ByteBuffer byteBuffer) {
    ConsensusGroupId consensusGroupId = ConsensusGroupId.deserialize(byteBuffer);
    List<Endpoint> endpointList = new ArrayList<>();
    int n = byteBuffer.getInt();
    for (int i = 0; i < n; i++) {
      Endpoint endpoint = new Endpoint();
      endpoint.deserializeImpl(byteBuffer);
      endpointList.add(endpoint);
    }
    return new RegionReplicaSet(consensusGroupId, endpointList);
  }
}
