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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.io.IOException;
import java.util.Arrays;

public class StepRegress {

  public static boolean useMad = TSFileDescriptor.getInstance().getConfig().isUseMad();

  // this is necessary, otherwise serialized twice by timeseriesMetadata and chunkMetadata
  // causing learn() executed more than once!!
  private boolean isLearned = false;

  private double slope = 0;

  // when learning parameters, we first determine segmentIntercepts and then determine segmentKeys;
  // when using functions, we read segmentKeys and then infer segmentIntercepts.
  // fix that the first segment is always tilt,
  // so for indexes starting from 0, even id is tilt, odd id is level.
  private DoubleArrayList segmentIntercepts = new DoubleArrayList(); // b1,b2,...,bm-1

  // fix that the first segment [t1,t2) is always tilt,
  // so t2=t1 in fact means that the first status is level
  private DoubleArrayList segmentKeys = new DoubleArrayList(); // t1,t2,...,tm
  //  deal with the last key tm

  private LongArrayList timestamps = new LongArrayList(); // Pi.t
  private LongArrayList intervals = new LongArrayList(); // Pi+1.t-Pi.t

  enum IntervalType {
    tilt,
    level
  }

  private IntArrayList intervalsType = new IntArrayList();
  private long previousTimestamp = -1;

  private double mean = 0; // mean of intervals
  private double stdDev = 0; // standard deviation of intervals
  public long count = 0;
  private double sumX2 = 0.0;
  private double sumX1 = 0.0;

  private double median = 0; // median of intervals
  private double mad = 0; // median absolute deviation of intervals
  TimeExactOrderStatistics statistics = new TimeExactOrderStatistics();

  /**
   * load data, record timestamps and intervals, preparing to calculate mean,std,median,mad along
   * the way
   */
  public void insert(long timestamp) {
    timestamps.add(timestamp); // record
    if (previousTimestamp > 0) {
      long delta = timestamp - previousTimestamp;
      intervals.add(delta); // record
      // prepare for mean and stdDev
      count++;
      sumX1 += delta;
      sumX2 += delta * delta;
      // prepare for median and mad
      statistics.insert(delta);
    }
    previousTimestamp = timestamp;
  }

  private void initForLearn() {
    this.mean = getMean();
    this.stdDev = getStdDev();
    this.median = getMedian();
    this.mad = getMad();
    this.slope = 1 / this.median;
    this.segmentKeys.add(timestamps.get(0)); // t1
    this.segmentIntercepts.add(1 - slope * timestamps.get(0)); // b1
  }

  /**
   * learn the parameters (slope and segmentKeys) of the step regression function for the loaded
   * data. Executed once and only once when serializing.
   */
  public void learn() throws IOException {
    if (isLearned) {
      // this is necessary, otherwise serialized twice by timeseriesMetadata and chunkMetadata
      // causing learn() executed more than once!!
      return;
    }
    isLearned = true;

    if (intervals.size() == 0) { // only one point
      this.segmentKeys.add(timestamps.get(0)); // t1
      return;
    }

    initForLearn();

    int tiltLatestSegmentID = 0;
    IntervalType previousIntervalType = IntervalType.tilt;

    for (int i = 0; i < intervals.size(); i++) {
      long delta = intervals.get(i);

      // the current point (t,pos) focused, where t is the left endpoint of the current interval.
      long t = timestamps.get(i);
      int pos = i + 1;
      // the next point (t,pos), where t is the right endpoint of the current interval.
      long nextT = timestamps.get(i + 1);
      int nextPos = i + 2;

      // 1) determine the type of the current interval
      // level condition: big interval && the right endpoint of the interval is under the latest
      // tilt line.
      // Note the right endpoint, not the left endpoint.
      // "the right endpoint of the interval is under the latest tilt line" is to ensure the
      // monotonically decreasing order of tilt intercepts (the last interval running through the
      // last point
      // is handled using post-processing to avoid disorder of tilt intercepts)
      boolean isLevel =
          isBigInterval(delta)
              && (nextPos < slope * nextT + segmentIntercepts.get(tiltLatestSegmentID));
      // to avoid TLTLTLTL... causing trivial segments, add extra rule for tilt
      if (!isLevel) {
        if (previousIntervalType == IntervalType.level) { // when previous interval is level
          if (i < intervals.size() - 1) { // when having next interval
            long nextDelta = intervals.get(i + 1);
            if (isBigInterval(nextDelta)
                && (nextPos + 1
                    < slope * timestamps.get(i + 2)
                        + segmentIntercepts.get(
                            tiltLatestSegmentID))) { // when next interval is also level
              isLevel = true; // then fix type from tilt to level, LTL=>LLL
            }
          }
        }
      }

      // 2) determine if starting a new segment
      if (isLevel) {
        intervalsType.add(IntervalType.level.ordinal());
        if (previousIntervalType == IntervalType.tilt) { // else do nothing, still level
          // [[[translate from tilt to level]]]
          previousIntervalType = IntervalType.level;
          // 3) to determine the intercept, let the level function run through (t,pos)
          double intercept = pos; // b2i=pos
          // 4) to determine the segment key, let the level function and the previous tilt function
          // intersect
          segmentKeys.add((intercept - segmentIntercepts.getLast()) / slope); // x2i=(b2i-b2i-1)/K
          // then add intercept to segmentIntercepts, do not change the order of codes here
          segmentIntercepts.add(intercept); //  debug if the first status is actually level works
        }
        // deal with the last interval to make sure the last point is hit
        //  create examples to debug this
        if (i == intervals.size() - 1) {
          // 3) to determine the intercept, let the level function run through
          // (timestamps.getLast(),timestamps.size())
          double intercept = timestamps.size(); // b2i=pos
          // 4) to determine the segment key, let the level function and the previous tilt function
          // intersect
          // Note that here is rewrite instead of add.
          // Note taht here is not getLast
          segmentKeys.set(
              segmentKeys.size() - 1,
              (intercept - segmentIntercepts.get(segmentIntercepts.size() - 2))
                  / slope); // x2i=(b2i-b2i-1)/K  debug here not getLast!
          // then add intercept to segmentIntercepts, do not change the order of codes here
          // Note that here is rewrite instead of add.
          segmentIntercepts.set(segmentIntercepts.size() - 1, intercept);
        }
      } else {
        intervalsType.add(IntervalType.tilt.ordinal());
        if (previousIntervalType == IntervalType.level) { // else do nothing, still tilt
          // [[[translate form level to tilt]]]
          previousIntervalType = IntervalType.tilt;
          // 3) to determine the intercept, let the tilt function run through (t,pos)
          double intercept = pos - slope * t; // b2i+1=pos-K*t
          // 4) to determine the segment key, let the level function and the previous tilt function
          // intersect
          segmentKeys.add((segmentIntercepts.getLast() - intercept) / slope); // x2i+1=(b2i-b2i+1)/K
          // then add intercept to segmentIntercepts, do not change the order of codes here
          segmentIntercepts.add(intercept);
          // remember to update tiltLatestSegmentID
          tiltLatestSegmentID += 2;
        }
        // deal with the last interval to make sure the last point is hit
        //  create examples to debug this
        if (i == intervals.size() - 1) {
          if (segmentIntercepts.size() == 1) { // all TTTTTT, only one segment info
            // remove all segment info, and directly connect the first and the last point
            this.slope = (timestamps.size() - 1.0) / (timestamps.getLast() - timestamps.getFirst());
            this.segmentKeys = new DoubleArrayList();
            this.segmentIntercepts = new DoubleArrayList();
            this.segmentKeys.add(timestamps.get(0)); // t1
            this.segmentIntercepts.add(1 - slope * timestamps.get(0)); // b1
          } else {
            // 3) to determine the intercept, let the tilt function run through
            // (timestamps.getLast(),timestamps.size())
            double intercept = timestamps.size() - slope * timestamps.getLast(); // b2i+1=pos-K*t
            // 4) to determine the segment key, let the level function and the previous tilt
            // function intersect
            // Note that here is rewrite instead of add.
            // Note taht here is not getLast
            segmentKeys.set(
                segmentKeys.size() - 1,
                (segmentIntercepts.get(segmentIntercepts.size() - 2) - intercept)
                    / slope); // x2i+1=(b2i-b2i+1)/K  debug here not getLast!
            // then add intercept to segmentIntercepts, do not change the order of codes here
            // Note that here is rewrite instead of add.
            segmentIntercepts.set(segmentIntercepts.size() - 1, intercept);

            // now check to remove possible disorders
            // search from back to front to find the first tilt intercept that is equal to or larger
            // than the current intercept
            int start = segmentIntercepts.size() - 3; //  debug
            //  consider only one T
            boolean equals = false;
            for (; start >= 0; start -= 2) {
              // note the step is 2, only tilt intercept, no level intercept
              if (segmentIntercepts.get(start) == intercept) {
                equals = true;
                break;
              }
              if (segmentIntercepts.get(start) > intercept) {
                equals = false;
                break;
              }
            }
            if (start < 0) { //  bug consider when start<0, i.e., not found: connecting directly
              // remove all segment info, and directly connect the first and the last point
              this.slope =
                  (timestamps.size() - 1.0) / (timestamps.getLast() - timestamps.getFirst());
              this.segmentKeys = new DoubleArrayList();
              this.segmentIntercepts = new DoubleArrayList();
              this.segmentKeys.add(timestamps.get(0)); // t1
              this.segmentIntercepts.add(1 - slope * timestamps.get(0)); // b1
            } else if ((start < segmentIntercepts.size() - 3) || equals) {
              // either the first tilt intercept which is not smaller than the last tilt intercept
              // is not the second-to-last tilt intercept,
              // or is the second-to-last tilt intercept but is equal to the last tilt intercept.
              if (!equals) {
                // remove all segment information after start+1 id, i.e., remove from start+2~end
                // note that the level after start tilt is kept since equals=false.
                segmentIntercepts =
                    DoubleArrayList.newListWith(
                        Arrays.copyOfRange(segmentIntercepts.toArray(), 0, start + 2));
                segmentKeys =
                    DoubleArrayList.newListWith(
                        Arrays.copyOfRange(segmentKeys.toArray(), 0, start + 2));

                // Add new segment info for TL&T
                // 4) to determine the segment key, let the level function and the previous tilt
                // function intersect
                // Note that here is add and getLast again!
                segmentKeys.add(
                    (segmentIntercepts.getLast() - intercept) / slope); // x2i+1=(b2i-b2i+1)/K
                // then add intercept to segmentIntercepts, do not change the order of codes here
                // Note that here is add and getLast again!
                segmentIntercepts.add(intercept);
              } else {
                // remove all segment information after start id, i.e., remove from start+1~end
                // note that the level after start tilt is NOT kept since equal==true
                segmentIntercepts =
                    DoubleArrayList.newListWith(
                        Arrays.copyOfRange(segmentIntercepts.toArray(), 0, start + 1));
                segmentKeys =
                    DoubleArrayList.newListWith(
                        Arrays.copyOfRange(segmentKeys.toArray(), 0, start + 1));
                //  debug the first status is level, b1
              }
            }
            // otherwise start==segmentIntercepts.size()-3 && equal=false,
            // means the first tilt intercept which is bigger than the last tilt intercept
            // is the second-to-last tilt intercept,
            // so in this case the result is already ready, no disorder to handle
          }
        }
      }
    }
    segmentKeys.add(timestamps.getLast()); // tm

    checkOrder();
  }

  /**
   * For id starting from 0, since we fix that the first status is always tilt, then intercepts with
   * even id should be monotonically decreasing, and intercepts with odd id should be monotonically
   * increasing.
   */
  private void checkOrder() throws IOException {
    double tiltIntercept = Double.MAX_VALUE;
    double levelIntercept = Double.MIN_VALUE;
    for (int i = 0; i < segmentIntercepts.size(); i++) {
      double intercept = segmentIntercepts.get(i);
      if (i % 2 == 0) {
        if (intercept >= tiltIntercept) {
          throw new IOException(
              String.format(
                  "disorder of tilt intercepts!: i=%s. Timestamps: %s, SegmentKeys: %s, SegmentIntercepts: %s",
                  i, timestamps, segmentKeys, segmentIntercepts));
        }
        tiltIntercept = intercept;
      } else {
        if (intercept <= levelIntercept) {
          throw new IOException(
              String.format(
                  "disorder of level intercepts!: i=%s. Timestamps: %s, SegmentKeys: %s, SegmentIntercepts: %s",
                  i, timestamps, segmentKeys, segmentIntercepts));
        }
        levelIntercept = intercept;
      }
    }
  }

  private boolean isBigInterval(long interval) {
    int bigIntervalParam = 3;
    if (!useMad) {
      return interval > this.mean + bigIntervalParam * this.stdDev;
    } else {
      return interval > this.median + bigIntervalParam * this.mad;
    }
  }

  public double getMedian() {
    return statistics.getMedian();
  }

  public double getMad() {
    return statistics.getMad();
  }

  public double getMean() { // sample mean
    return sumX1 / count;
  }

  public double getStdDev() { // sample standard deviation
    double std = Math.sqrt(this.sumX2 / this.count - Math.pow(this.sumX1 / this.count, 2));
    return Math.sqrt(Math.pow(std, 2) * this.count / (this.count - 1));
  }

  public DoubleArrayList getSegmentIntercepts() {
    return segmentIntercepts;
  }

  public double getSlope() {
    return slope;
  }

  public void setSlope(double slope) {
    this.slope = slope;
  }

  public void setSegmentKeys(DoubleArrayList segmentKeys) {
    this.segmentKeys = segmentKeys;
  }

  public DoubleArrayList getSegmentKeys() {
    return segmentKeys;
  }

  public IntArrayList getIntervalsType() {
    return intervalsType;
  }

  public LongArrayList getIntervals() {
    return intervals;
  }

  public LongArrayList getTimestamps() {
    return timestamps;
  }

  /**
   * infer m-1 intercepts b1,b2,...,bm-1 given the slope and m segmentKeys t1,t2,...,tm (tm is not
   * used) Executed once and only once when deserializing.
   */
  public void inferInterceptsFromSegmentKeys() {
    segmentIntercepts.add(1 - slope * segmentKeys.get(0)); // b1=1-K*t1
    for (int i = 1; i < segmentKeys.size() - 1; i++) { // b2,b3,...,bm-1
      if (i % 2 == 0) { // b2i+1=b2i-1-K*(t2i+1-t2i)
        double b = segmentIntercepts.get(segmentIntercepts.size() - 2);
        segmentIntercepts.add(b - slope * (segmentKeys.get(i) - segmentKeys.get(i - 1)));
      } else { // b2i=K*t2i+b2i-1
        double b = segmentIntercepts.getLast();
        segmentIntercepts.add(slope * segmentKeys.get(i) + b);
      }
    }
  }

  public int infer(double t, int count) throws IOException {
    int pos = (int) Math.round(infer_internal(t)); // starting from 1
    // As double loses precision, although the last point is passed at the phase of learning,
    // the inferred pos by rounding may still exceed max position count.
    if (pos > count) {
      pos--;
    }
    return pos;
  }

  /**
   * @param t input timestamp
   * @return output the value of the step regression function f(t), which is the estimated position
   *     in the chunk. Pay attention that f(t) starts from (startTime,1), ends at (endTime,count).
   */
  public double infer_internal(double t) throws IOException {
    if (segmentKeys.size() == 1) { //  DEBUG
      return 1;
    }

    if (t < segmentKeys.get(0) || t > segmentKeys.getLast()) {
      throw new IOException(
          String.format(
              "t out of range. input within [%s,%s]", segmentKeys.get(0), segmentKeys.getLast()));
    }
    int seg = binarySearch(segmentKeys, t);
    // we have fixed that the first status is always tilt,
    // so for indexes starting from 0, even id is tilt, odd id is level.
    if (seg % 2 == 0) { // tilt
      return slope * t + segmentIntercepts.get(seg);
    } else {
      return segmentIntercepts.get(seg);
    }
  }

  // find firstly strictly greater than or equal to the element in a sorted array
  public int binarySearch(DoubleArrayList segmentKeys, double targetT) {
    int start = 0;
    int end = segmentKeys.size() - 1;
    int ans = -1;
    while (start <= end) {
      int mid = (start + end) / 2;
      // Move to right side if target is greater.
      if (segmentKeys.get(mid) < targetT) {
        start = mid + 1;
      } else // Move left side.
      {
        ans = mid;
        end = mid - 1;
      }
    }
    if (ans == 0) {
      return ans; // means that targetT equals the first segment keys, therefore ans is not the
      // right point of the segment
    } else {
      return ans - 1; // the id of the segment
    }
  }
}
