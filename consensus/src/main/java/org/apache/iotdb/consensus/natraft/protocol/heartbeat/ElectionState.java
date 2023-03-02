package org.apache.iotdb.consensus.natraft.protocol.heartbeat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.consensus.common.Peer;

public class ElectionState {
  private List<Peer> currNodes;
  private List<Peer> newNodes;
  private Set<Peer> acceptedCurrNodes;
  private Set<Peer> acceptedNewNodes;
  private Set<Peer> rejectedCurrNodes;
  private Set<Peer> rejectedNewNodes;
  private volatile boolean accepted = false;
  private volatile boolean rejected = false;

  public ElectionState(List<Peer> currNodes, List<Peer> newNodes) {
    this.currNodes = currNodes;
    this.newNodes = newNodes;
    acceptedCurrNodes = new HashSet<>(currNodes.size());
    rejectedCurrNodes = new HashSet<>(currNodes.size());
    acceptedNewNodes = newNodes != null ? new HashSet<>(newNodes.size()) : null;
    rejectedNewNodes = newNodes != null ? new HashSet<>(newNodes.size()) : null;
  }

  public void onAccept(Peer node) {
    if (currNodes.contains(node)) {
      acceptedCurrNodes.add(node);
    }
    if (newNodes != null && newNodes.contains(node)) {
      acceptedNewNodes.add(node);
    }
    if (acceptedCurrNodes.size() >= currNodes.size() / 2 &&
        (newNodes == null || (acceptedNewNodes.size() >= newNodes.size() / 2))) {
      accepted = true;
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  public void onReject(Peer node) {
    if (currNodes.contains(node)) {
      rejectedCurrNodes.add(node);
    }
    if (newNodes != null && newNodes.contains(node)) {
      rejectedNewNodes.add(node);
    }
    if (rejectedCurrNodes.size() >= currNodes.size() / 2 + 1 &&
        (newNodes == null || (rejectedNewNodes.size() >= newNodes.size() / 2 + 1))) {
      rejected = true;
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  public List<Peer> getCurrNodes() {
    return currNodes;
  }

  public List<Peer> getNewNodes() {
    return newNodes;
  }

  public boolean isAccepted() {
    return accepted;
  }

  public boolean isRejected() {
    return rejected;
  }

  public void setRejected(boolean rejected) {
    this.rejected = rejected;
    synchronized (this) {
      this.notifyAll();
    }
  }
}
