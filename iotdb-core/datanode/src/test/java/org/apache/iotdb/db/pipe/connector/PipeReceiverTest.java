package org.apache.iotdb.db.pipe.connector;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.connector.v1.IoTDBThriftReceiverV1;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferTabletReq;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.mock;

public class PipeReceiverTest {
  @Test
  public void testIoTDBThriftReceiverV1() {
    IoTDBThriftReceiverV1 receiver = new IoTDBThriftReceiverV1();
    try {
      receiver.receive(
          PipeTransferHandshakeReq.toTPipeTransferReq(
              CommonDescriptor.getInstance().getConfig().getTimestampPrecision()),
          mock(IPartitionFetcher.class),
          mock(ISchemaFetcher.class));
      receiver.receive(
          PipeTransferTabletReq.toTPipeTransferReq(
              new Tablet(
                  "root.sg.d",
                  Collections.singletonList(new MeasurementSchema("s", TSDataType.INT32))),
              true),
          mock(IPartitionFetcher.class),
          mock(ISchemaFetcher.class));
    } catch (IOException e) {
      Assert.fail();
    }
  }
}
