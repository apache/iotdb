package org.apache.iotdb.confignode.procedure.impl.pipe.task;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreatePipeProcedureV2Test {
  @Test
  public void serializeDeserializeTest() throws PipeException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    Map<String, String> collectorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();
    collectorAttributes.put("collector", "org.apache.iotdb.pipe.collector.DefaultCollector");
    processorAttributes.put("processor", "org.apache.iotdb.pipe.processor.SDTFilterProcessor");
    connectorAttributes.put("connector", "org.apache.iotdb.pipe.protocal.ThriftTransporter");

    CreatePipeProcedureV2 proc =
        new CreatePipeProcedureV2(
            new TCreatePipeReq()
                .setPipeName("testPipe")
                .setCollectorAttributes(collectorAttributes)
                .setProcessorAttributes(processorAttributes)
                .setConnectorAttributes(connectorAttributes));

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      CreatePipeProcedureV2 proc2 =
          (CreatePipeProcedureV2) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }
}
