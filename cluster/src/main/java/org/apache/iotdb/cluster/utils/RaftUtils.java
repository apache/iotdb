package org.apache.iotdb.cluster.utils;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.iotdb.cluster.entity.raft.RaftNode;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;

public class RaftUtils {

  private RaftUtils(){
  }

  /**
   * Get leader node according to the group id
   *
   * @param groupId group id of raft group
   * @return PeerId of leader
   */
  public static PeerId getLeader(String groupId) throws RaftConnectionException {
    Configuration conf = getConfiguration(groupId);
    RouteTable.getInstance().updateConfiguration(groupId, conf);

    final BoltCliClientService cliClientService = new BoltCliClientService();
    cliClientService.init(new CliOptions());
    try {
      if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
        throw new RaftConnectionException("Refresh leader failed");
      }
    } catch (InterruptedException|TimeoutException e) {
      throw new RaftConnectionException("Refresh leader failed");
    }
    return RouteTable.getInstance().selectLeader(groupId);
  }


  public static Configuration getConfiguration(String groupID) {
    return null;
  }

  public static List<RaftNode> convertNodesToRaftNodeList(String[] nodes) {
    List<RaftNode> nodeList = new ArrayList<>(nodes.length);
    for (int i = 0; i < nodes.length; i++) {
      nodeList.add(RaftNode.parseRaftNode(nodes[i]));
    }
    return nodeList;
  }

  public static int getIndexOfIpFromRaftNodeList(String ip, List<RaftNode> nodeList) {
    for (int i = 0; i < nodeList.size(); i++) {
      if (nodeList.get(i).getIp().equals(ip)) {
        return i;
      }
    }
    return -1;
  }

  public static PeerId convertPhysicalNode(PhysicalNode node){
    return new PeerId(node.ip, node.port);
  }

  public static PhysicalNode convertPeerId(PeerId peer){
    return new PhysicalNode(peer.getIp(), peer.getPort());
  }

}
