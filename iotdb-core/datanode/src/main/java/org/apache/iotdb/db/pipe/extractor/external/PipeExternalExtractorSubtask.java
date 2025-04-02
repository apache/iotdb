//package org.apache.iotdb.db.pipe.extractor.external;
//
//import com.google.common.util.concurrent.ListeningExecutorService;
//import org.apache.iotdb.commons.consensus.DataRegionId;
//import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
//import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
//import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
//import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskScheduler;
//import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
//import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeSubtask;
//import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
//import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
//import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtask;
//import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtaskWorkerManager;
//import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
//import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
//import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeRemainingEventAndTimeMetrics;
//import org.apache.iotdb.db.pipe.metric.processor.PipeProcessorMetrics;
//import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
//import org.apache.iotdb.db.pipe.processor.pipeconsensus.PipeConsensusProcessor;
//import org.apache.iotdb.db.storageengine.StorageEngine;
//import org.apache.iotdb.db.utils.ErrorHandlingUtils;
//import org.apache.iotdb.pipe.api.PipeProcessor;
//import org.apache.iotdb.pipe.api.event.Event;
//import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
//import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
//import org.apache.iotdb.pipe.api.exception.PipeException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Objects;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.atomic.AtomicReference;
//
//public class PipeExternalExtractorSubtask extends PipeSubtask {
//    private static final Logger LOGGER = LoggerFactory.getLogger(PipeExternalExtractorSubtask.class);
////TODO
////    private static final AtomicReference<PipeProcessorSubtaskWorkerManager> subtaskWorkerManager =
////            new AtomicReference<>();
//
//    // Record these variables to provide corresponding value to tag key of monitoring metrics
//    private final String pipeName;
//    private final String pipeNameWithCreationTime; // cache for better performance
//    private final int regionId;
//
//    private final EventSupplier inputEventSupplier;
//
//    // This variable is used to distinguish between old and new subtasks before and after stuck
//    // restart.
//    private final long subtaskCreationTime;
//    protected final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue;
//    public PipeExternalExtractorSubtask(
//            final String taskID,
//            final String pipeName,
//            final long creationTime,
//            final int regionId,
//            final EventSupplier inputEventSupplier,
//            final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue) {
//        super(taskID, creationTime);
//        this.pipeName = pipeName;
//        this.pipeNameWithCreationTime = pipeName + "_" + creationTime;
//        this.regionId = regionId;
//        this.inputEventSupplier = inputEventSupplier;
//        this.subtaskCreationTime = System.currentTimeMillis();
//        this.pendingQueue = pendingQueue;
//    }
//    @Override
//    public void bindExecutors(ListeningExecutorService subtaskWorkerThreadPoolExecutor, ExecutorService subtaskCallbackListeningExecutor, PipeSubtaskScheduler subtaskScheduler) {
//        this.subtaskWorkerThreadPoolExecutor = subtaskWorkerThreadPoolExecutor;
//        this.subtaskScheduler = subtaskScheduler;
//
////        // double check locking for constructing PipeProcessorSubtaskWorkerManager
////        if (subtaskWorkerManager.get() == null) {
////            synchronized (PipeProcessorSubtaskWorkerManager.class) {
////                if (subtaskWorkerManager.get() == null) {
////                    subtaskWorkerManager.set(
////                            new PipeProcessorSubtaskWorkerManager(subtaskWorkerThreadPoolExecutor));
////                }
////            }
////        }
////        subtaskWorkerManager.get().schedule(this);
//
//    }
//
//    @Override
//    protected boolean executeOnce() throws Exception {
//        if (isClosed.get()) {
//            return false;
//        }
//
//        final Event event =
//                lastEvent != null
//                        ? lastEvent
//                        : UserDefinedEnrichedEvent.maybeOf(inputEventSupplier.supply());
//        // Record the last event for retry when exception occurs
//        setLastEvent(event);
//
//        if (Objects.isNull(event)) {
//            return false;
//        }
//
//        try {
//            if (event instanceof EnrichedEvent) {
//                ((EnrichedEvent) event).throwIfNoPrivilege();
//            }
//            // event can be supplied after the subtask is closed, so we need to check isClosed here
//            if (!isClosed.get()) {
//                if (event instanceof TabletInsertionEvent) {
//                    pipeProcessor.process((TabletInsertionEvent) event, outputEventCollector);
//                    PipeProcessorMetrics.getInstance().markTabletEvent(taskID);
//                } else if (event instanceof TsFileInsertionEvent) {
//                    pipeProcessor.process((TsFileInsertionEvent) event, outputEventCollector);
//                    PipeProcessorMetrics.getInstance().markTsFileEvent(taskID);
//                    PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
//                            .markTsFileCollectInvocationCount(
//                                    pipeNameWithCreationTime, outputEventCollector.getCollectInvocationCount());
//                } else if (event instanceof PipeHeartbeatEvent) {
//                    pipeProcessor.process(event, outputEventCollector);
//                    ((PipeHeartbeatEvent) event).onProcessed();
//                    PipeProcessorMetrics.getInstance().markPipeHeartbeatEvent(taskID);
//                } else {
//                    pipeProcessor.process(
//                            event instanceof UserDefinedEnrichedEvent
//                                    ? ((UserDefinedEnrichedEvent) event).getUserDefinedEvent()
//                                    : event,
//                            outputEventCollector);
//                }
//            }
//
//            final boolean shouldReport =
//                    !isClosed.get()
//                            // If an event does not generate any events except itself at this stage, it is divided
//                            // into two categories:
//                            // 1. If the event is collected and passed to the connector, the reference count of
//                            // the event may eventually be zero in the processor (the connector reduces the
//                            // reference count first, and then the processor reduces the reference count), at this
//                            // time, the progress of the event needs to be reported.
//                            // 2. If the event is not collected (not passed to the connector), the reference count
//                            // of the event must be zero in the processor stage, at this time, the progress of the
//                            // event needs to be reported.
//                            && outputEventCollector.hasNoGeneratedEvent()
//                            // If the event's reference count cannot be increased, it means that the event has
//                            // been released, and the progress of the event can not be reported.
//                            && !outputEventCollector.isFailedToIncreaseReferenceCount()
//                            // Events generated from consensusPipe's transferred data should never be reported.
//                            && !(pipeProcessor instanceof PipeConsensusProcessor);
//            if (shouldReport
//                    && event instanceof EnrichedEvent
//                    && outputEventCollector.hasNoCollectInvocationAfterReset()) {
//                // An event should be reported here when it is not passed to the connector stage, and it
//                // does not generate any new events to be passed to the connector. In our system, before
//                // reporting an event, we need to enrich a commitKey and commitId, which is done in the
//                // collector stage. But for the event that not passed to the connector and not generate any
//                // new events, the collector stage is not triggered, so we need to enrich the commitKey and
//                // commitId here.
//                PipeEventCommitManager.getInstance()
//                        .enrichWithCommitterKeyAndCommitId((EnrichedEvent) event, creationTime, regionId);
//            }
//            decreaseReferenceCountAndReleaseLastEvent(event, shouldReport);
//        } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
//            LOGGER.info(
//                    "Temporarily out of memory in pipe event processing, will wait for the memory to release.",
//                    e);
//            return false;
//        } catch (final Exception e) {
//            if (!isClosed.get()) {
//                throw new PipeException(
//                        String.format(
//                                "Exception in pipe process, subtask: %s, last event: %s, root cause: %s",
//                                taskID,
//                                lastEvent instanceof EnrichedEvent
//                                        ? ((EnrichedEvent) lastEvent).coreReportMessage()
//                                        : lastEvent,
//                                ErrorHandlingUtils.getRootCause(e).getMessage()),
//                        e);
//            } else {
//                LOGGER.info("Exception in pipe event processing, ignored because pipe is dropped.", e);
//                clearReferenceCountAndReleaseLastEvent(event);
//            }
//        }
//
//        return true;
//    }
//
//    @Override
//    public void submitSelf() {
//
//    }
//
//    @Override
//    public void onFailure(Throwable t) {
//
//    }
//}
