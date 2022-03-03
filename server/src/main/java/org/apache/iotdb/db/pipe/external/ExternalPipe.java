/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.external;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.newsync.datasource.PipeOpManager;
import org.apache.iotdb.db.newsync.datasource.PipeStorageGroupInfo;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.pipe.external.operation.InsertOperation;
import org.apache.iotdb.db.pipe.external.operation.Operation;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.pipe.external.api.DataType;
import org.apache.iotdb.pipe.external.api.ExternalPipeSinkWriterStatus;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriter;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Math.abs;

/**
 * This class handles 1 external plugin's work, including multiple working threads. All
 * StorageGroups will be distributed to different thread according to hash value. Every working
 * thread is responsible for put several StorageGroups' data into external sink.
 */
public class ExternalPipe {
  private static final Logger logger = LoggerFactory.getLogger(ExternalPipe.class);

  private String extPipeTypeName;
  // ParamKey => ParamValue
  Map<String, String> sinkParams;
  private PipeOpManager pipeOpManager;

  private IExternalPipeSinkWriterFactory pipeSinkWriterFactory;
  private ExternalPipeConfiguration configuration;

  private volatile boolean alive = false;
  private List<DataTransmissionTask> dataTransmissionTasks;
  private ExecutorService executorService;

  // SG => DataCommitIndex
  private Map<String, Long> dataCommitMap = new ConcurrentHashMap<>();
  // Writer method name => (exception message => count)
  private Map<String, Map<String, AtomicInteger>> writerInvocationFailures;
  private int timestampDivisor;

  public ExternalPipe(
      String extPipeTypeName, Map<String, String> sinkParams, PipeOpManager pipeOpManager) {
    this.extPipeTypeName = extPipeTypeName;
    this.sinkParams = sinkParams;
    this.pipeOpManager = pipeOpManager;

    String timePrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();
    switch (timePrecision) {
      case "ms":
        timestampDivisor = 1;
        break;
      case "us":
        timestampDivisor = 1_000;
        break;
      case "ns":
        timestampDivisor = 1_000_000;
        break;
      default:
        throw new IllegalArgumentException("Unrecognized time precision: " + timePrecision);
    }
  }

  @TestOnly
  public ExternalPipe(
      String Name,
      IExternalPipeSinkWriterFactory factory,
      ExternalPipeConfiguration conf,
      TsFilePipe tsFilePipe) {}

  @TestOnly
  public void setPipeSinkWriterFactory(IExternalPipeSinkWriterFactory pipeSinkWriterFactory) {
    this.pipeSinkWriterFactory = pipeSinkWriterFactory;
  }

  /**
   * Get the parameter value from sinkParams that is from CMD
   *
   * @param paramName
   * @param defaultValue
   * @return
   */
  private int getIntParam(String paramName, int defaultValue) {
    String valueStr = sinkParams.get(paramName);
    if (valueStr == null) {
      return defaultValue;
    }
    return Integer.parseInt(valueStr);
  }

  /**
   * start and init all working threads
   *
   * @throws IOException
   */
  public void start() throws IOException {
    logger.debug("ExternalPipe start(), extPipeName={}.", extPipeTypeName);

    if (alive) {
      String errMsg = "Can not re-run alive External pipe: " + extPipeTypeName + ".";
      logger.error(errMsg);
      throw new IllegalStateException(errMsg);
    }

    int threadNum = getIntParam("thread_num", ExternalPipeConfiguration.DEFAULT_NUM_OF_THREADS);
    int batchSize =
        getIntParam("batch_size", ExternalPipeConfiguration.DEFAULT_OPERATION_BATCH_SIZE);
    int attemptTimes =
        getIntParam("attempt_times", ExternalPipeConfiguration.DEFAULT_ATTEMPT_TIMES);
    int backoffInterval =
        getIntParam("retry_interval", ExternalPipeConfiguration.DEFAULT_BACKOFF_INTERVAL);

    try {
      // == Pipe configuration
      configuration =
          new ExternalPipeConfiguration.Builder(extPipeTypeName)
              .numOfThreads(threadNum)
              .operationBatchSize(batchSize)
              .attemptTimes(attemptTimes)
              .backOffInterval(backoffInterval)
              .build();

      if (pipeSinkWriterFactory == null) {
        pipeSinkWriterFactory =
            ExternalPipePluginManager.getInstance().getWriteFactory(extPipeTypeName);
      }

      // Try to init externalPipeSinkWriterFactory, also check the PIPESink parameter
      pipeSinkWriterFactory.initialize(sinkParams);
    } catch (Exception e) {
      logger.error("Failed to start External Pipe: {}.", extPipeTypeName, e);
      throw new IOException(
          "Failed to start External Pipe: " + extPipeTypeName + ". " + e.getMessage());
    }

    alive = true;
    logger.info("External pipe " + extPipeTypeName + " begin to START");

    // == Launch pipe worker threads
    executorService =
        Executors.newFixedThreadPool(
            threadNum,
            r -> {
              Thread thread = new Thread(r);
              thread.setName("ExternalPipe-worker-" + extPipeTypeName + "-" + thread.getId());
              return thread;
            });

    // == Start threads that will run external PiPeSink plugin
    dataTransmissionTasks = new ArrayList<>(threadNum);
    for (int i = 0; i < threadNum; i++) {
      IExternalPipeSinkWriter writer = pipeSinkWriterFactory.get();
      DataTransmissionTask dataTransmissionTask =
          new DataTransmissionTask(writer, i, configuration);
      dataTransmissionTasks.add(dataTransmissionTask);
      executorService.submit(dataTransmissionTask);
    }

    writerInvocationFailures = new ConcurrentHashMap<>();

    logger.info("External pipe " + extPipeTypeName + " finish START.");
  }

  /** Stop all working threads */
  public void stop() {
    if (!alive) {
      String errMsg = "Error: External pipe " + extPipeTypeName + " has not started.";
      logger.error(errMsg);
      throw new IllegalStateException(errMsg);
    }
    alive = false;

    executorService.shutdown();
    boolean isExecutorServiceTerminated = false;
    try {
      isExecutorServiceTerminated = executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error(
          "Interrupted when waiting for the termination of external pipe, " + extPipeTypeName, e);
    } finally {
      if (!isExecutorServiceTerminated) {
        logger.warn(
            "ExternalPipe stop(), graceful termination of external pipe {} timed out. So force terminating working threads.",
            extPipeTypeName);
        executorService.shutdownNow();
      }
    }
  }

  public boolean isAlive() {
    return alive;
  }

  /**
   * Collect the status of External Pipe.
   *
   * @return
   */
  public ExternalPipeStatus getStatus() {
    ExternalPipeStatus status = new ExternalPipeStatus();

    try {
      List<ExternalPipeSinkWriterStatus> writerStatuses =
          dataTransmissionTasks.stream()
              .map(DataTransmissionTask::getStatus)
              .collect(Collectors.toList());
      status.setWriterStatuses(writerStatuses);
    } catch (Exception e) {
      handleExceptionsThrownByWriter("getStatus", e);
    }

    status.setAlive(alive);
    status.setWriterInvocationFailures(writerInvocationFailures);
    return status;
  }

  private void handleExceptionsThrownByWriter(String method, Exception e) {
    writerInvocationFailures.computeIfAbsent(method, m -> new ConcurrentHashMap<>());
    String eMsg = e.getMessage();
    if (eMsg == null) {
      eMsg = "N/A";
    }
    writerInvocationFailures.get(method).computeIfAbsent(eMsg, msg -> new AtomicInteger(0));
    writerInvocationFailures.get(method).get(eMsg).incrementAndGet();
    logger.info("Exception thrown form writer", e);
  }

  /**
   * Get the data committed index of dedicated StorageGroup
   *
   * @param sgName
   * @return
   */
  public long getDataCommitIndex(String sgName) {
    return dataCommitMap.getOrDefault(sgName, Long.MIN_VALUE);
  }

  /**
   * working thread class Every thread take 1 IExternalPipeSinkWriter and is responsible for several
   * StorageGroup's data
   */
  private class DataTransmissionTask implements Callable<Void> {

    private final IExternalPipeSinkWriter writer;
    private final int threadIndex;
    private final ExternalPipeConfiguration configuration;

    // SG ==> PipeStorageGroupInfo
    private Map<String, PipeStorageGroupInfo> sgInfoMap;

    private long nextIndex;
    private String lastReadSgName;

    DataTransmissionTask(
        IExternalPipeSinkWriter writer, int threadIndex, ExternalPipeConfiguration configuration)
        throws IOException {
      this.writer = Validate.notNull(writer);
      this.threadIndex = threadIndex;
      this.configuration = configuration;

      this.sgInfoMap = configuration.getBucketSgInfoMap(threadIndex);

      this.writer.open();
    }

    /**
     * Get 1 SG's next data index that will be read according to past reading record. If there is no
     * SG in local record, get the first valid data index in PIPE data source.
     *
     * @param sgName
     * @return
     */
    private long getSgNextDataIndex(String sgName) {
      PipeStorageGroupInfo sgInfo = sgInfoMap.get(sgName);
      if (sgInfo != null) {
        return sgInfo.getNextReadIndex();
      }

      long nextReadIndex = pipeOpManager.getCommittedIndex(sgName) + 1;
      sgInfoMap.put(sgName, new PipeStorageGroupInfo(sgName, nextReadIndex - 1, nextReadIndex));

      return nextReadIndex;
    }

    /**
     * save the data index info to local record.
     *
     * @param sgName
     * @param nextReadIndex
     * @param committedIndex
     */
    private void setSgNextDataIndex(String sgName, long nextReadIndex, long committedIndex) {
      logger.debug(
          "setSgNextDataIndex(), sgName={}, nextReadIndex={}, committedIndex={}.",
          sgName,
          nextReadIndex,
          committedIndex);

      PipeStorageGroupInfo sgInfo =
          sgInfoMap.computeIfAbsent(sgName, k -> new PipeStorageGroupInfo(sgName, -1, 0));

      sgInfo.setNextReadIndex(nextReadIndex);
      sgInfo.setCommittedIndex(committedIndex);
    }

    /**
     * commit data to let Pipe data source may remove useless data.
     *
     * @param sgName
     * @param committedIndex
     */
    private void commitData(String sgName, long committedIndex) {
      logger.debug("commitData(), sgName={}, committedIndex={}.", sgName, committedIndex);

      dataCommitMap.put(sgName, committedIndex);
    }

    /**
     * check whether PIPE data source has new data for dedicate StorageGroup
     *
     * @param sgName
     * @return
     */
    private boolean sgHasNewData(String sgName) {
      PipeStorageGroupInfo pipeStorageGroupInfo = sgInfoMap.get(sgName);
      if (pipeStorageGroupInfo == null) {
        return true;
      }

      long nextReadIndex = pipeStorageGroupInfo.getNextReadIndex();
      long nextIndex = pipeOpManager.getNextIndex(sgName);
      if (nextIndex > nextReadIndex) {
        return true;
      }

      return false;
    }

    /**
     * Check/Wait new data/operation
     *
     * @return StorageGroup Name that has new data
     * @throws InterruptedException
     */
    public String waitForOperations() throws InterruptedException {
      if (lastReadSgName != null) {
        if (sgHasNewData(lastReadSgName)) {
          return lastReadSgName;
        }
        lastReadSgName = null;
      }

      // find new data for the StorageGroup in sgSet
      Set<String> sgSet = pipeOpManager.getSgSet();
      while (alive) {
        Iterator<String> iter = sgSet.iterator();
        while (iter.hasNext()) {
          String sgName = iter.next();
          if (threadIndex != (abs(sgName.hashCode()) % configuration.getNumOfThreads())) {
            continue;
          }

          if (sgHasNewData(sgName)) {
            lastReadSgName = sgName;
            return sgName;
          }
        }

        Thread.sleep(1_000); // 1 seconds
      }
      return null;
    }

    /**
     * Working thread Entrance. Read data from pipe and send data to external sink.
     *
     * @return
     * @throws Exception
     */
    @Override
    public Void call() throws Exception {
      logger.info("ExternalPipeWorker start. thread={}.", Thread.currentThread().getName());

      while (alive) {
        try {
          // String sgName = pipeOpManager.waitForOperations(threadIndex, sgInfoMap);
          String sgName = waitForOperations();
          if (sgName == null) {
            continue;
          }

          Operation operation = null;
          try {
            operation =
                pipeOpManager.getOperation(
                    sgName, getSgNextDataIndex(sgName), configuration.getOperationBatchSize());
          } catch (IOException e) {
            continue;
          }

          if ((operation == null) || (operation.getDataCount() <= 0)) {
            continue;
          }

          if (!handleOperationWithRetry(operation)) {
            logger.error(
                "Failed to handle operation after "
                    + configuration.getAttemptTimes()
                    + " attempts: "
                    + operation);
          }

          if (!flushWithRetry()) {
            logger.error(
                "Failed to flush operations after "
                    + configuration.getAttemptTimes()
                    + " attempts: startIndex="
                    + operation.getStartIndex()
                    + ",endIndex="
                    + operation.getEndIndex());
          }
          // TODO: Reminder, May cause data loss if the flush failed.

          // Update the next index and commit to the pipe source.
          nextIndex = operation.getEndIndex();
          setSgNextDataIndex(sgName, nextIndex, nextIndex - 1);
          commitData(sgName, nextIndex - 1);
        } catch (InterruptedException e) {
          break;
        } catch (Exception e) {
          logger.error("Unexpected system exception", e);
        }
      }

      try {
        writer.close();
      } catch (IOException e) {
        handleExceptionsThrownByWriter("close", e);
        logger.info("Exception happened when closing the writer", e);
      }

      logger.info("ExternalPipeWorker exits. Thread={}", Thread.currentThread().getName());
      return null;
    }

    public ExternalPipeSinkWriterStatus getStatus() {
      return writer.getStatus();
    }

    private boolean handleOperationWithRetry(Operation operation) {
      if (operation instanceof InsertOperation) {
        try {
          handleInsertOperation((InsertOperation) operation);
        } catch (Exception e1) {
          logger.error("Exception happened when handling an insert operation", e1);
          handleExceptionsThrownByWriter("insert", e1);
          boolean succeed = false;
          int attemptTimes = 1;
          while (alive && attemptTimes < configuration.getAttemptTimes()) {
            // Backoff
            try {
              Thread.sleep(configuration.getBackOffInterval());
            } catch (InterruptedException ignored) {
            }
            // Retry
            try {
              handleInsertOperation((InsertOperation) operation);
              succeed = true;
              break;
            } catch (Exception e2) {
              handleExceptionsThrownByWriter("insert", e2);
            }
            attemptTimes += 1;
          }
          return succeed;
        }
      } else {
        throw new IllegalArgumentException(
            "Unrecognized operation " + operation.getClass().getSimpleName());
      }
      return true;
    }

    private void handleInsertOperation(InsertOperation operation) throws IOException {
      for (Pair<MeasurementPath, List<TimeValuePair>> dataPair : operation.getDataList()) {
        MeasurementPath path = dataPair.left;
        for (TimeValuePair tvPair : dataPair.right) {
          String[] nodes = path.getNodes();
          long timestampInMs = tvPair.getTimestamp() / timestampDivisor;
          switch (tvPair.getValue().getDataType()) {
            case BOOLEAN:
              writer.insertBoolean(nodes, timestampInMs, tvPair.getValue().getBoolean());
              break;
            case INT32:
              writer.insertInt32(nodes, timestampInMs, tvPair.getValue().getInt());
              break;
            case INT64:
              writer.insertInt64(nodes, timestampInMs, tvPair.getValue().getLong());
              break;
            case FLOAT:
              writer.insertFloat(nodes, timestampInMs, tvPair.getValue().getFloat());
              break;
            case DOUBLE:
              writer.insertDouble(nodes, timestampInMs, tvPair.getValue().getDouble());
              break;
            case TEXT:
              writer.insertText(nodes, timestampInMs, tvPair.getValue().getStringValue());
              break;
            case VECTOR:
              writer.insertVector(
                  nodes,
                  Arrays.stream(tvPair.getValue().getVector())
                      .map(TsPrimitiveType::getDataType)
                      .map(type -> DataType.fromTsDataType(type.serialize()))
                      .toArray(DataType[]::new),
                  timestampInMs,
                  Arrays.stream(tvPair.getValue().getVector())
                      .map(TsPrimitiveType::getValue)
                      .toArray(Object[]::new));
              break;
            default:
              throw new IllegalArgumentException(
                  "Unrecognized data type " + tvPair.getValue().getDataType());
          }
        }
      }
    }

    private boolean flushWithRetry() {
      try {
        writer.flush();
      } catch (IOException e1) {
        logger.error("Exception happened when flushing operations", e1);
        handleExceptionsThrownByWriter("flush", e1);
        boolean succeed = false;
        int attemptTimes = 1;
        while (alive && attemptTimes < configuration.getAttemptTimes()) {
          // Backoff
          try {
            Thread.sleep(configuration.getBackOffInterval());
          } catch (InterruptedException ignored) {
          }
          // Retry
          try {
            writer.flush();
            succeed = true;
            break;
          } catch (Exception e2) {
            handleExceptionsThrownByWriter("flush", e2);
          }
          attemptTimes += 1;
        }
        return succeed;
      }
      return true;
    }
  }
}
