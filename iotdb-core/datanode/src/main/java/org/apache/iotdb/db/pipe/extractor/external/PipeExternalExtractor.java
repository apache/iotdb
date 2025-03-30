package org.apache.iotdb.db.pipe.extractor.external;

import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PipeExternalExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeExternalExtractor.class);

  protected String pipeName;
  protected long creationTime;
  protected PipeTaskMeta pipeTaskMeta;
  protected final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

  protected final AtomicBoolean hasBeenStarted = new AtomicBoolean(false);
  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  protected String deviceId = "root.sg.d1";
  final long[] times = new long[] {110L, 111L, 112L, 113L, 114L};
  final String[] measurementIds =
      new String[] {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};
  final TSDataType[] dataTypes =
      new TSDataType[] {
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.BOOLEAN,
        TSDataType.TEXT,
        TSDataType.TIMESTAMP,
        TSDataType.DATE,
        TSDataType.BLOB,
        TSDataType.STRING,
      };

  final MeasurementSchema[] schemas = new MeasurementSchema[10];

  final String pattern = "root.sg.d1";

  Tablet tabletForInsertRowNode;
  Tablet tabletForInsertTabletNode;

  Thread pollingThread;

  protected AtomicLong index = new AtomicLong(0);

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    for (int i = 0; i < schemas.length; i++) {
      schemas[i] = new MeasurementSchema(measurementIds[i], dataTypes[i]);
    }

    // create tablet for insertRowNode

  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeTaskMeta = environment.getPipeTaskMeta();
    deviceId = "root.sg.d" + -environment.getRegionId();
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get()) {
      return;
    }

    hasBeenStarted.set(true);

    pollingThread = new Thread(this::pollExternalSource);
    pollingThread.start();
  }

  private void pollExternalSource() {
    try {
      Random rand = new Random();
      while (!isClosed.get() && !Thread.currentThread().isInterrupted()) {
        BitMap[] bitMapsForInsertRowNode = new BitMap[schemas.length];
        for (int i = 0; i < schemas.length; i++) {
          bitMapsForInsertRowNode[i] = new BitMap(1);
        }
        final Object[] values = new Object[schemas.length];
        values[0] = new int[1];
        values[1] = new long[1];
        values[2] = new float[1];
        values[3] = new double[1];
        values[4] = new boolean[1];
        values[5] = new Binary[1];
        values[6] = new long[times.length];
        values[7] = new LocalDate[times.length];
        values[8] = new Binary[times.length];
        values[9] = new Binary[times.length];

        for (int r = 0; r < 1; r++) {
          ((int[]) values[0])[r] = rand.nextInt();
          ((long[]) values[1])[r] = rand.nextLong();
          ((float[]) values[2])[r] = rand.nextFloat();
          ((double[]) values[3])[r] = rand.nextDouble();
          ((boolean[]) values[4])[r] = rand.nextBoolean();
          ((Binary[]) values[5])[r] = BytesUtils.valueOf("string");
          ((long[]) values[6])[r] = rand.nextLong();
          ((LocalDate[]) values[7])[r] = LocalDate.now();
          ((Binary[]) values[8])[r] = BytesUtils.valueOf("blob");
          ((Binary[]) values[9])[r] = BytesUtils.valueOf("string");
        }

        tabletForInsertRowNode =
            new Tablet(
                deviceId,
                Arrays.asList(schemas),
                new long[] {System.currentTimeMillis()},
                values,
                bitMapsForInsertRowNode,
                1);
        PipeRawTabletInsertionEvent event =
            new PipeRawTabletInsertionEvent(
                false,
                "root.sg",
                null,
                null,
                tabletForInsertRowNode,
                true,
                pipeName,
                creationTime,
                pipeTaskMeta,
                null,
                false);
        if (!event.increaseReferenceCount(PipeExternalExtractor.class.getName())) {
          LOGGER.warn(
              "The reference count of the event {} cannot be increased, skipping it.", event);
          continue;
        }
        pendingQueue.waitedOffer(event);
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOGGER.error("Error polling external source", e);
    }
  }

  @Override
  public Event supply() throws Exception {
    if (isClosed.get()) {
      return null;
    }
    EnrichedEvent event = pendingQueue.directPoll();
    if (Objects.nonNull(event)) {
      event.bindProgressIndex(new IoTProgressIndex(1, index.getAndIncrement()));
    }
    return event;
  }

  @Override
  public void close() throws Exception {
    isClosed.set(true);

    if (pollingThread != null) {
      pollingThread.interrupt();
      pollingThread = null;
    }

    pendingQueue.clear();
  }
}
