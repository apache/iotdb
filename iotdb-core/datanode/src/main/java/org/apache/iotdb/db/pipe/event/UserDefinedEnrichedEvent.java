package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.UserDefinedEvent;

public class UserDefinedEnrichedEvent extends EnrichedEvent {

  private final UserDefinedEvent userDefinedEvent;
  private final EnrichedEvent enrichedEvent;

  public static Event maybeOf(Event event) {
    return event instanceof UserDefinedEvent
            && ((UserDefinedEvent) event).getSourceEvent() instanceof EnrichedEvent
        ? new UserDefinedEnrichedEvent(
            (UserDefinedEvent) event, (EnrichedEvent) ((UserDefinedEvent) event).getSourceEvent())
        : event;
  }

  private UserDefinedEnrichedEvent(UserDefinedEvent userDefinedEvent, EnrichedEvent enrichedEvent) {
    super(
        enrichedEvent.getPipeName(),
        enrichedEvent.getPipeTaskMeta(),
        enrichedEvent.getPattern(),
        enrichedEvent.getStartTime(),
        enrichedEvent.getEndTime());
    this.userDefinedEvent = userDefinedEvent;
    this.enrichedEvent = enrichedEvent;
  }

  public UserDefinedEvent getUserDefinedEvent() {
    return userDefinedEvent;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    return enrichedEvent.internallyIncreaseResourceReferenceCount(holderMessage);
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    return enrichedEvent.internallyDecreaseResourceReferenceCount(holderMessage);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return enrichedEvent.getProgressIndex();
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName, PipeTaskMeta pipeTaskMeta, String pattern, long startTime, long endTime) {
    return enrichedEvent.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
        pipeName, pipeTaskMeta, pattern, startTime, endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return enrichedEvent.isGeneratedByPipe();
  }

  @Override
  public boolean isEventTimeOverlappedWithTimeRange() {
    return enrichedEvent.isEventTimeOverlappedWithTimeRange();
  }
}
