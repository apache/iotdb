package org.apache.iotdb.commons.exception.pipe;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class PipeRuntimeExceptionTest {
  @Test
  public void testPipeRuntimeNonCriticalException() {
    PipeRuntimeNonCriticalException e = new PipeRuntimeNonCriticalException("test");
    Assert.assertEquals(new PipeRuntimeNonCriticalException("test"), e);
    ByteBuffer buffer = ByteBuffer.allocate(32);
    e.serialize(buffer);
    buffer.position(0);
    try {
      PipeRuntimeNonCriticalException e1 =
          (PipeRuntimeNonCriticalException) PipeRuntimeExceptionType.deserializeFrom(buffer);
      Assert.assertEquals(e.hashCode(), e1.hashCode());
    } catch (ClassCastException classCastException) {
      Assert.fail();
    }
  }

  @Test
  public void testPipeRuntimeCriticalException() {
    PipeRuntimeCriticalException e = new PipeRuntimeCriticalException("test");
    Assert.assertEquals(new PipeRuntimeCriticalException("test"), e);
    ByteBuffer buffer = ByteBuffer.allocate(32);
    e.serialize(buffer);
    buffer.position(0);
    try {
      PipeRuntimeCriticalException e1 =
          (PipeRuntimeCriticalException) PipeRuntimeExceptionType.deserializeFrom(buffer);
      Assert.assertEquals(e.hashCode(), e1.hashCode());
    } catch (ClassCastException classCastException) {
      Assert.fail();
    }
  }

  @Test
  public void testPipeRuntimeConnectorCriticalException() {
    PipeRuntimeConnectorCriticalException e = new PipeRuntimeConnectorCriticalException("test");
    Assert.assertEquals(new PipeRuntimeConnectorCriticalException("test"), e);
    ByteBuffer buffer = ByteBuffer.allocate(32);
    e.serialize(buffer);
    buffer.position(0);
    try {
      PipeRuntimeConnectorCriticalException e1 =
          (PipeRuntimeConnectorCriticalException) PipeRuntimeExceptionType.deserializeFrom(buffer);
      Assert.assertEquals(e.hashCode(), e1.hashCode());
    } catch (ClassCastException classCastException) {
      Assert.fail();
    }
  }
}
