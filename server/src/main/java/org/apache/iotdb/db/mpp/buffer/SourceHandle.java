package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.mpp.common.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.Validate;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

public class SourceHandle implements ISourceHandle {

  private final long bufferCapacityInBytes;

  private final Queue<TsBlock> bufferedTsBlocks = new ArrayDeque<>();
  private volatile SettableFuture<Void> blocked = SettableFuture.create();
  private volatile long bufferRetainedSizeInBytes;
  private boolean finished;
  private boolean closed;
  private Throwable throwable;

  public SourceHandle(long bufferCapacityInBytes) {
    Validate.isTrue(bufferCapacityInBytes > 0L, "capacity cannot be less or equal to zero.");
    this.bufferCapacityInBytes = bufferCapacityInBytes;
  }

  @Override
  public TsBlock receive() {
    if (throwable != null) {
      throw new RuntimeException(throwable);
    }
    if (closed) {
      throw new IllegalStateException("Source handle has been closed.");
    }
    TsBlock tsBlock = bufferedTsBlocks.poll();
    if (tsBlock != null) {
      bufferRetainedSizeInBytes -= getRetainedSizeInBytes(tsBlock);
    }
    if (bufferedTsBlocks.isEmpty() && !finished && blocked.isDone()) {
      blocked = SettableFuture.create();
    }
    return tsBlock;
  }

  private long getRetainedSizeInBytes(TsBlock tsBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  public ListenableFuture<Void> isBlocked() {
    return nonCancellationPropagating(blocked);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    bufferedTsBlocks.clear();
    bufferRetainedSizeInBytes = 0;
    closed = true;
    if (!blocked.isDone()) {}
  }
}
