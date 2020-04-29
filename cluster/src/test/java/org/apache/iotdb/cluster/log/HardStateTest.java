package org.apache.iotdb.cluster.log;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.junit.Test;

public class HardStateTest {

  @Test
  public void testHardState() {
    // Not NULL
    HardState state = new HardState();
    state.setCurrentTerm(2);
    state.setVoteFor(new Node("127.0.0.1", 30000, 0, 40000));
    ByteBuffer buffer = state.serialize();
    HardState newState = HardState.deserialize(buffer);
    assertEquals(state, newState);

    // NULL
    state.setVoteFor(null);
    buffer = state.serialize();
    newState = HardState.deserialize(buffer);
    assertEquals(state, newState);
  }
}