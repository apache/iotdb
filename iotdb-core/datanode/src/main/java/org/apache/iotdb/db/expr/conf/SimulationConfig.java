package org.apache.iotdb.db.expr.conf;

public class SimulationConfig {

  // if the time range is [0, 1000], then the file size is 10_000 bytes
  public long timeRangeToBytesFactor = 10L;
  // when using us timestamp, 10MB/s = 10B/us
  public long IoBandwidthBytesPerTimestamp = 10;
  // assume 2ms seek time
  public long IoSeekTimestamp = 2_000;
  public long deletionSizeInByte = 50;

  public long modFileSizeThreshold = 16 * 1024;
  public int modFileCntThreshold = 10;

  public long generateTsFileInterval = 10_000_000L;
  public long tsfileRange = 10_000_000L;

  // the first query/deletion occurs after writing 10 files
  public long deletionStartTime = tsfileRange * 10;

  public long generatePartialDeletionInterval = 20_000_000L;
  public long generateFullDeletionInterval = 20_000_000L;
  // the first deletion ranges from [partialDeletionOffset, partialDeletionRange +
  // partialDeletionOffset],
  // and the next one ranges from [partialDeletionOffset + partialDeletionStep, partialDeletionRange
  // + partialDeletionOffset + partialDeletionStep],
  // and so on
  public long partialDeletionRange = tsfileRange * 3;
  public long partialDeletionStep = tsfileRange / 2;
  public long partialDeletionOffset = -partialDeletionRange;

  public long rangeQueryInterval = 50_000;
  public long rangeQueryRange = tsfileRange * 10;
  public long rangeQueryOffset = rangeQueryRange;
  // if a tsFile is generated every X queries, then the step should be 1/X of the file range,
  // so that the query end time will not exceed data time
  public long rangeQueryStep = tsfileRange / (generateTsFileInterval / rangeQueryInterval);

  public long fullQueryInterval = 50_000;
  public long pointQueryInterval = 50_000;

  public double writeTimeWeight = 100.0;
}
