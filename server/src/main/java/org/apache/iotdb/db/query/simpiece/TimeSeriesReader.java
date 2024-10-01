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

import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4Tri;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class TimeSeriesReader {

  public static TimeSeries getTimeSeriesFromTsFiles(
      List<ChunkSuit4Tri> chunkSuit4TriList, long startTime, long endTime) throws IOException {
    // assume chunkSuit4TriList already sorted in increasing time order
    ArrayList<Point> ts = new ArrayList<>();
    double max = Double.MIN_VALUE;
    double min = Double.MAX_VALUE;

    int start = 0;

    for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
      TSDataType dataType = chunkSuit4Tri.chunkMetadata.getDataType();
      if (dataType != TSDataType.DOUBLE) {
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
      ChunkMetadata chunkMetadata = chunkSuit4Tri.chunkMetadata;
      long chunkMinTime = chunkMetadata.getStartTime();
      long chunkMaxTime = chunkMetadata.getEndTime();
      if (chunkMaxTime < startTime) {
        continue;
      } else if (chunkMinTime >= endTime) {
        break;
      } else {
        chunkSuit4Tri.globalStartInList = start; // pointer start
        chunkSuit4Tri.lastReadPos = start; // note this means global pos in the list
        PageReader pageReader = // note this pageReader and its buffer is not maintained in memory
            FileLoaderUtils.loadPageReaderList4CPV(
                chunkSuit4Tri.chunkMetadata,
                null); // note do not assign to chunkSuit4Tri.pageReader
        for (int j = 0; j < chunkSuit4Tri.chunkMetadata.getStatistics().getCount(); j++) {
          long timestamp = pageReader.timeBuffer.getLong(j * 8);
          if (timestamp < startTime) {
            continue;
          } else if (timestamp >= endTime) {
            break;
          } else { // rightStartTime<=t<rightEndTime
            ByteBuffer valueBuffer = pageReader.valueBuffer;
            double value = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
            ts.add(new Point(timestamp, value));
            max = Math.max(max, value);
            min = Math.min(min, value);
            start++;
          }
        }
        chunkSuit4Tri.globalEndInList = start; // pointer end
      }
    }
    return new TimeSeries(ts, max - min);
  }

  public static double[] getTimeSeriesFromTsFilesDR(
      List<ChunkSuit4Tri> chunkSuit4TriList, long startTime, long endTime) throws IOException {
    // assume chunkSuit4TriList already sorted in increasing time order
    List<Double> ts = new ArrayList<>();

    int start = 0;

    for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
      TSDataType dataType = chunkSuit4Tri.chunkMetadata.getDataType();
      if (dataType != TSDataType.DOUBLE) {
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
      ChunkMetadata chunkMetadata = chunkSuit4Tri.chunkMetadata;
      long chunkMinTime = chunkMetadata.getStartTime();
      long chunkMaxTime = chunkMetadata.getEndTime();
      if (chunkMaxTime < startTime) {
        continue;
      } else if (chunkMinTime >= endTime) {
        break;
      } else {
        chunkSuit4Tri.globalStartInList = start; // pointer start
        chunkSuit4Tri.lastReadPos = start; // note this means global pos in the list
        PageReader pageReader = // note this pageReader and its buffer is not maintained in memory
            FileLoaderUtils.loadPageReaderList4CPV(
                chunkSuit4Tri.chunkMetadata,
                null); // note do not assign to chunkSuit4Tri.pageReader
        for (int j = 0; j < chunkSuit4Tri.chunkMetadata.getStatistics().getCount(); j++) {
          long timestamp = pageReader.timeBuffer.getLong(j * 8);
          if (timestamp < startTime) {
            continue;
          } else if (timestamp >= endTime) {
            break;
          } else { // rightStartTime<=t<rightEndTime
            ByteBuffer valueBuffer = pageReader.valueBuffer;
            double value = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
            ts.add(value);
            start++;
          }
        }
        chunkSuit4Tri.globalEndInList = start; // pointer end
      }
    }

    return ts.stream().mapToDouble(Double::doubleValue).toArray();
  }

  public static List<VisvalPoint> getTimeSeriesFromTsFilesVisval(
      List<ChunkSuit4Tri> chunkSuit4TriList, long startTime, long endTime) throws IOException {
    // assume chunkSuit4TriList already sorted in increasing time order
    ArrayList<VisvalPoint> ts = new ArrayList<>();
    double max = Double.MIN_VALUE;
    double min = Double.MAX_VALUE;

    for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
      ChunkMetadata chunkMetadata = chunkSuit4Tri.chunkMetadata;
      long chunkMinTime = chunkMetadata.getStartTime();
      long chunkMaxTime = chunkMetadata.getEndTime();
      if (chunkMaxTime < startTime) {
        continue;
      } else if (chunkMinTime >= endTime) {
        break;
      } else {
        PageReader pageReader =
            FileLoaderUtils.loadPageReaderList4CPV(
                chunkSuit4Tri.chunkMetadata,
                null); // note do not assign to chunkSuit4Tri.pageReader
        for (int j = 0; j < chunkSuit4Tri.chunkMetadata.getStatistics().getCount(); j++) {
          long timestamp = pageReader.timeBuffer.getLong(j * 8);
          if (timestamp < startTime) {
            continue;
          } else if (timestamp >= endTime) {
            break;
          } else { // rightStartTime<=t<rightEndTime
            ByteBuffer valueBuffer = pageReader.valueBuffer;
            double value = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
            ts.add(new VisvalPoint(timestamp, value));
            max = Math.max(max, value);
            min = Math.min(min, value);
          }
        }
      }
    }
    return ts;
  }

  public static TimeSeries getTimeSeries(InputStream inputStream, String delimiter, boolean gzip) {
    ArrayList<Point> ts = new ArrayList<>();
    double max = Double.MIN_VALUE;
    double min = Double.MAX_VALUE;

    try {
      if (gzip) {
        inputStream = new GZIPInputStream(inputStream);
      }
      Reader decoder = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
      BufferedReader bufferedReader = new BufferedReader(decoder);

      String line;
      while ((line = bufferedReader.readLine()) != null) {
        String[] elements = line.split(delimiter);
        long timestamp = Long.parseLong(elements[0]);
        double value = Double.parseDouble(elements[1]);
        ts.add(new Point(timestamp, value));

        max = Math.max(max, value);
        min = Math.min(min, value);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new TimeSeries(ts, max - min);
  }

  public static TimeSeries getMyTimeSeries(
      InputStream inputStream,
      String delimiter,
      boolean gzip,
      int N,
      int startRow,
      boolean hasHeader,
      boolean seriesTimeColumn) {
    // N<0 means read all lines

    ArrayList<Point> ts = new ArrayList<>();
    double max = Double.MIN_VALUE;
    double min = Double.MAX_VALUE;

    try {
      if (gzip) {
        inputStream = new GZIPInputStream(inputStream);
      }
      Reader decoder = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
      BufferedReader bufferedReader = new BufferedReader(decoder);

      String line;
      if (hasHeader) {
        bufferedReader.readLine();
      }
      int startCnt = 0;
      while (startCnt < startRow && (line = bufferedReader.readLine()) != null) {
        startCnt++;
      }
      if (startCnt < startRow) {
        throw new IOException("not enough rows!");
      }
      int cnt = 0;
      while ((cnt < N || N < 0) && (line = bufferedReader.readLine()) != null) {
        String[] elements = line.split(delimiter);
        long timestamp;
        if (!seriesTimeColumn) {
          timestamp = (long) Double.parseDouble(elements[0]);
        } else {
          timestamp = cnt; // for DFT, DWT, OM3
        }
        double value = Double.parseDouble(elements[1]);
        ts.add(new Point(timestamp, value));

        max = Math.max(max, value);
        min = Math.min(min, value);
        cnt++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new TimeSeries(ts, max - min);
  }

  public static List<VisvalPoint> getMyTimeSeriesVisval(
      InputStream inputStream,
      String delimiter,
      boolean gzip,
      int N,
      int startRow,
      boolean hasHeader,
      boolean seriesTimeColumn) {
    // N<0 means read all lines

    ArrayList<VisvalPoint> ts = new ArrayList<>();
    double max = Double.MIN_VALUE;
    double min = Double.MAX_VALUE;

    try {
      if (gzip) {
        inputStream = new GZIPInputStream(inputStream);
      }
      Reader decoder = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
      BufferedReader bufferedReader = new BufferedReader(decoder);

      String line;
      if (hasHeader) {
        bufferedReader.readLine();
      }
      int startCnt = 0;
      while (startCnt < startRow && (line = bufferedReader.readLine()) != null) {
        startCnt++;
      }
      if (startCnt < startRow) {
        throw new IOException("not enough rows!");
      }
      int cnt = 0;
      while ((cnt < N || N < 0) && (line = bufferedReader.readLine()) != null) {
        String[] elements = line.split(delimiter);
        long timestamp;
        if (!seriesTimeColumn) {
          timestamp = (long) Double.parseDouble(elements[0]);
        } else {
          timestamp = cnt + 1;
        }
        double value = Double.parseDouble(elements[1]);
        ts.add(new VisvalPoint(timestamp, value));

        max = Math.max(max, value);
        min = Math.min(min, value);
        cnt++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ts;
  }
}
