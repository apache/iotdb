package org.apache.iotdb.db.subscription.event;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import java.util.List;

public class TsFileSubscriptionEvent extends SubscriptionEvent {

  private final String topicName;
  private final String fileName;

  private final long fileLength;
  private final long startWritingOffset;
  private final byte[] filePiece;

  public TsFileSubscriptionEvent(
      final List<EnrichedEvent> enrichedEvents,
      final String subscriptionCommitId,
      final String topicName,
      final String fileName) {
    super(enrichedEvents, subscriptionCommitId);

    this.topicName = topicName;
    this.fileName = fileName;

    this.fileLength = -1;
    this.startWritingOffset = -1;
    this.filePiece = null;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileLength() {
    return fileLength;
  }

  public long getStartWritingOffset() {
    return startWritingOffset;
  }

  public byte[] getFilePiece() {
    return filePiece;
  }
}
