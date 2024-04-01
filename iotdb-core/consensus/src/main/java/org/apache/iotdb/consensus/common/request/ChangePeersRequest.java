package org.apache.iotdb.consensus.common.request;

import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ChangePeersRequest implements IConsensusRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangePeersRequest.class);

  private List<Peer> peers;

  public ChangePeersRequest() {}

  public ChangePeersRequest(List<Peer> peers) {
    this.peers = peers;
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      outputStream.writeInt(peers == null ? 0 : peers.size());
      if (peers != null) {
        for (Peer peer : peers) {
          peer.serialize(outputStream);
        }
      }
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      LOGGER.error("Unexpected error occurs when serializing {}.", getClass().getName(), e);
      throw new SerializationRunTimeException(e);
    }
  }

  public static ChangePeersRequest deserialize(ByteBuffer buffer) {
    ChangePeersRequest request = new ChangePeersRequest();
    List<Peer> peers = new ArrayList<>();
    int peerCnt = buffer.getInt();
    for (int i = 0; i < peerCnt; i++) {
      peers.add(Peer.deserialize(buffer));
    }
    request.peers = peers;
    return request;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.CHANGE_PEERS;
  }

  public List<Peer> getPeers() {
    return peers;
  }
}
