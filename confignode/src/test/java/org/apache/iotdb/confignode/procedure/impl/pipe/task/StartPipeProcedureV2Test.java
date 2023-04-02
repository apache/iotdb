package org.apache.iotdb.confignode.procedure.impl.pipe.task;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StartPipeProcedureV2Test {
  @Test
  public void serializeDeserializeTest() throws PipeException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    StartPipeProcedureV2 proc = new StartPipeProcedureV2("testPipe");

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      StartPipeProcedureV2 proc2 =
          (StartPipeProcedureV2) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }
}
