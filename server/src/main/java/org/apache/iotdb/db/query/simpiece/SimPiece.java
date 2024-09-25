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

// Sim-Piece code forked from https://github.com/xkitsios/Sim-Piece.git

package org.apache.iotdb.db.query.simpiece;

import org.apache.iotdb.db.query.simpiece.Encoding.FloatEncoder;
import org.apache.iotdb.db.query.simpiece.Encoding.UIntEncoder;
import org.apache.iotdb.db.query.simpiece.Encoding.VariableByteEncoder;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class SimPiece {
  public ArrayList<SimPieceSegment> segments;

  private double epsilon;
  public long lastTimeStamp;

  public SimPiece(List<Point> points, double epsilon) throws IOException {
    if (points.isEmpty()) throw new IOException();

    this.epsilon = epsilon;
    this.lastTimeStamp = points.get(points.size() - 1).getTimestamp();
    this.segments = mergePerB(compress(points));
  }

  //  public SimPiece(byte[] bytes, boolean variableByte, boolean zstd) throws IOException {
  //    readByteArray(bytes, variableByte, zstd);
  //  }

  private double quantization(double value) {
    return Math.round(value / epsilon) * epsilon;
  }

  private int createSegment(int startIdx, List<Point> points, ArrayList<SimPieceSegment> segments) {
    long initTimestamp = points.get(startIdx).getTimestamp();
    double b = quantization(points.get(startIdx).getValue());
    if (startIdx + 1 == points.size()) {
      segments.add(new SimPieceSegment(initTimestamp, -Double.MAX_VALUE, Double.MAX_VALUE, b));
      return startIdx + 1;
    }
    double aMax =
        ((points.get(startIdx + 1).getValue() + epsilon) - b)
            / (points.get(startIdx + 1).getTimestamp() - initTimestamp);
    double aMin =
        ((points.get(startIdx + 1).getValue() - epsilon) - b)
            / (points.get(startIdx + 1).getTimestamp() - initTimestamp);
    if (startIdx + 2 == points.size()) {
      segments.add(new SimPieceSegment(initTimestamp, aMin, aMax, b));
      return startIdx + 2;
    }

    for (int idx = startIdx + 2; idx < points.size(); idx++) {
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;

      double upValue = points.get(idx).getValue() + epsilon;
      double downValue = points.get(idx).getValue() - epsilon;

      double upLim = aMax * (points.get(idx).getTimestamp() - initTimestamp) + b;
      double downLim = aMin * (points.get(idx).getTimestamp() - initTimestamp) + b;
      if ((downValue > upLim || upValue < downLim)) {
        segments.add(new SimPieceSegment(initTimestamp, aMin, aMax, b));
        return idx;
      }

      if (upValue < upLim)
        aMax = Math.max((upValue - b) / (points.get(idx).getTimestamp() - initTimestamp), aMin);
      if (downValue > downLim)
        aMin = Math.min((downValue - b) / (points.get(idx).getTimestamp() - initTimestamp), aMax);
    }
    segments.add(new SimPieceSegment(initTimestamp, aMin, aMax, b));

    return points.size();
  }

  private ArrayList<SimPieceSegment> compress(List<Point> points) {
    ArrayList<SimPieceSegment> segments = new ArrayList<>();
    int currentIdx = 0;
    while (currentIdx < points.size()) currentIdx = createSegment(currentIdx, points, segments);

    return segments;
  }

  private ArrayList<SimPieceSegment> mergePerB(ArrayList<SimPieceSegment> segments) {
    double aMinTemp = -Double.MAX_VALUE;
    double aMaxTemp = Double.MAX_VALUE;
    double b = Double.NaN;
    ArrayList<Long> timestamps = new ArrayList<>();
    ArrayList<SimPieceSegment> mergedSegments = new ArrayList<>();

    segments.sort(
        Comparator.comparingDouble(SimPieceSegment::getB)
            .thenComparingDouble(SimPieceSegment::getA));
    for (int i = 0; i < segments.size(); i++) {
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      if (b != segments.get(i).getB()) {
        if (timestamps.size() == 1)
          mergedSegments.add(new SimPieceSegment(timestamps.get(0), aMinTemp, aMaxTemp, b));
        else {
          for (Long timestamp : timestamps)
            mergedSegments.add(new SimPieceSegment(timestamp, aMinTemp, aMaxTemp, b));
        }
        timestamps.clear();
        timestamps.add(segments.get(i).getInitTimestamp());
        aMinTemp = segments.get(i).getAMin();
        aMaxTemp = segments.get(i).getAMax();
        b = segments.get(i).getB();
        continue;
      }
      if (segments.get(i).getAMin() <= aMaxTemp && segments.get(i).getAMax() >= aMinTemp) {
        timestamps.add(segments.get(i).getInitTimestamp());
        aMinTemp = Math.max(aMinTemp, segments.get(i).getAMin());
        aMaxTemp = Math.min(aMaxTemp, segments.get(i).getAMax());
      } else {
        if (timestamps.size() == 1) mergedSegments.add(segments.get(i - 1));
        else {
          for (long timestamp : timestamps)
            mergedSegments.add(new SimPieceSegment(timestamp, aMinTemp, aMaxTemp, b));
        }
        timestamps.clear();
        timestamps.add(segments.get(i).getInitTimestamp());
        aMinTemp = segments.get(i).getAMin();
        aMaxTemp = segments.get(i).getAMax();
      }
    }
    if (!timestamps.isEmpty()) {
      if (timestamps.size() == 1)
        mergedSegments.add(new SimPieceSegment(timestamps.get(0), aMinTemp, aMaxTemp, b));
      else {
        for (long timestamp : timestamps)
          mergedSegments.add(new SimPieceSegment(timestamp, aMinTemp, aMaxTemp, b));
      }
    }

    return mergedSegments;
  }

  public List<Point> decompress() {
    segments.sort(Comparator.comparingLong(SimPieceSegment::getInitTimestamp));
    List<Point> points = new ArrayList<>();
    long currentTimeStamp = segments.get(0).getInitTimestamp();

    for (int i = 0; i < segments.size() - 1; i++) {
      while (currentTimeStamp < segments.get(i + 1).getInitTimestamp()) {
        points.add(
            new Point(
                currentTimeStamp,
                segments.get(i).getA() * (currentTimeStamp - segments.get(i).getInitTimestamp())
                    + segments.get(i).getB()));
        currentTimeStamp++;
      }
    }

    while (currentTimeStamp <= lastTimeStamp) {
      points.add(
          new Point(
              currentTimeStamp,
              segments.get(segments.size() - 1).getA()
                      * (currentTimeStamp - segments.get(segments.size() - 1).getInitTimestamp())
                  + segments.get(segments.size() - 1).getB()));
      currentTimeStamp++;
    }

    return points;
  }

  private void toByteArrayPerBSegments(
      ArrayList<SimPieceSegment> segments, boolean variableByte, ByteArrayOutputStream outStream)
      throws IOException {
    TreeMap<Integer, HashMap<Double, ArrayList<Long>>> input = new TreeMap<>();
    for (SimPieceSegment segment : segments) {
      double a = segment.getA();
      int b = (int) Math.round(segment.getB() / epsilon);
      long t = segment.getInitTimestamp();
      if (!input.containsKey(b)) input.put(b, new HashMap<>());
      if (!input.get(b).containsKey(a)) input.get(b).put(a, new ArrayList<>());
      input.get(b).get(a).add(t);
    }

    VariableByteEncoder.write(input.size(), outStream);
    if (input.isEmpty()) return;
    int previousB = input.firstKey();
    VariableByteEncoder.write(previousB, outStream);
    for (Map.Entry<Integer, HashMap<Double, ArrayList<Long>>> bSegments : input.entrySet()) {
      VariableByteEncoder.write(bSegments.getKey() - previousB, outStream);
      previousB = bSegments.getKey();
      VariableByteEncoder.write(bSegments.getValue().size(), outStream);
      for (Map.Entry<Double, ArrayList<Long>> aSegment : bSegments.getValue().entrySet()) {
        FloatEncoder.write(aSegment.getKey().floatValue(), outStream);
        if (variableByte) Collections.sort(aSegment.getValue());
        VariableByteEncoder.write(aSegment.getValue().size(), outStream);
        long previousTS = 0;
        for (Long timestamp : aSegment.getValue()) {
          if (variableByte) VariableByteEncoder.write((int) (timestamp - previousTS), outStream);
          else UIntEncoder.write(timestamp, outStream);
          previousTS = timestamp;
        }
      }
    }
  }

  //
  //  public byte[] toByteArray(boolean variableByte, boolean zstd) throws IOException {
  //    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
  //    byte[] bytes;
  //
  //    FloatEncoder.write((float) epsilon, outStream);
  //
  //    toByteArrayPerBSegments(segments, variableByte, outStream);
  //
  //    if (variableByte) VariableByteEncoder.write((int) lastTimeStamp, outStream);
  //    else UIntEncoder.write(lastTimeStamp, outStream);
  //
  //    if (zstd) bytes = Zstd.compress(outStream.toByteArray());
  //    else bytes = outStream.toByteArray();
  //
  //    outStream.close();
  //
  //    return bytes;
  //  }

  private ArrayList<SimPieceSegment> readMergedPerBSegments(
      boolean variableByte, ByteArrayInputStream inStream) throws IOException {
    ArrayList<SimPieceSegment> segments = new ArrayList<>();
    long numB = VariableByteEncoder.read(inStream);
    if (numB == 0) return segments;
    int previousB = VariableByteEncoder.read(inStream);
    for (int i = 0; i < numB; i++) {
      int b = VariableByteEncoder.read(inStream) + previousB;
      previousB = b;
      int numA = VariableByteEncoder.read(inStream);
      for (int j = 0; j < numA; j++) {
        float a = FloatEncoder.read(inStream);
        int numTimestamps = VariableByteEncoder.read(inStream);
        long timestamp = 0;
        for (int k = 0; k < numTimestamps; k++) {
          if (variableByte) timestamp += VariableByteEncoder.read(inStream);
          else timestamp = UIntEncoder.read(inStream);
          segments.add(new SimPieceSegment(timestamp, a, (float) (b * epsilon)));
        }
      }
    }

    return segments;
  }

  //  private void readByteArray(byte[] input, boolean variableByte, boolean zstd) throws
  // IOException {
  //    byte[] binary;
  //    if (zstd) binary = Zstd.decompress(input, input.length * 2); //TODO: How to know apriori
  // original size?
  //    else binary = input;
  //    ByteArrayInputStream inStream = new ByteArrayInputStream(binary);
  //
  //    this.epsilon = FloatEncoder.read(inStream);
  //    this.segments = readMergedPerBSegments(variableByte, inStream);
  //    if (variableByte) this.lastTimeStamp = VariableByteEncoder.read(inStream);
  //    else this.lastTimeStamp = UIntEncoder.read(inStream);
  //    inStream.close();
  //  }
}
