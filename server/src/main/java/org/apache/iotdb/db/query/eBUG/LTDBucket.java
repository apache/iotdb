package org.apache.iotdb.db.query.eBUG;

public class LTDBucket {

  public int startIdx, endIdx;
  public double sse, sumOf2SSE;

  public LTDBucket prev;
  public LTDBucket next;

  public boolean isDeleted;

  public LTDBucket(int startIdx, int endIdx, double sse, double sumOf2SSE) {
    this.startIdx = startIdx;
    this.endIdx = endIdx;
    this.sse = sse;
    this.sumOf2SSE = sumOf2SSE;
    this.prev = null;
    this.next = null;
    this.isDeleted = false;
  }

  public LTDBucket(LTDBucket ltdBucket) {
    this.startIdx = ltdBucket.startIdx;
    this.endIdx = ltdBucket.endIdx;
    this.sse = ltdBucket.sse;
    this.sumOf2SSE = ltdBucket.sumOf2SSE;
    this.prev = ltdBucket.prev;
    this.next = ltdBucket.next;
    this.isDeleted = false;

    if (ltdBucket.prev != null) {
      ltdBucket.prev.next = this;
    }
    if (ltdBucket.next != null) {
      ltdBucket.next.prev = this;
    }
  }

  public int getStartIdx() {
    return startIdx;
  }

  public int getEndIdx() {
    return endIdx;
  }

  public int getMergedEndIdx() {
    if (this.next == null) {
      return this.endIdx;
    } else {
      return this.next.endIdx;
    }
  }

  public double getNextSSE() {
    if (this.next == null) {
      return Double.MAX_VALUE;
    } else {
      return this.next.sse;
    }
  }

  @Override
  public String toString() {
    return "[" + startIdx + ", " + endIdx + "]: " + sse + "," + sumOf2SSE;
  }
}
