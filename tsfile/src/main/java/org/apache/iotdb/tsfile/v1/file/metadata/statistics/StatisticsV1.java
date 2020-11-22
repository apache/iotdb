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
package org.apache.iotdb.tsfile.v1.file.metadata.statistics;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.BinaryStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.BooleanStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.v1.file.metadata.ChunkMetadataV1;
import org.apache.iotdb.tsfile.v1.file.metadata.TsDigestV1;

/**
 * This class is used for recording statistic information of each measurement in a delta file. While
 * writing processing, the processor records the digest information. Statistics includes maximum,
 * minimum and null value count up to version 0.0.1.<br> Each data type extends this Statistic as
 * super class.<br>
 *
 * @param <T> data type for Statistics
 */
public abstract class StatisticsV1<T> {

  /**
   * static method providing statistic instance for respective data type.
   *
   * @param type - data type
   * @return Statistics
   */
  public static StatisticsV1 getStatsByType(TSDataType type) {
    switch (type) {
      case INT32:
        return new IntegerStatisticsV1();
      case INT64:
        return new LongStatisticsV1();
      case TEXT:
        return new BinaryStatisticsV1();
      case BOOLEAN:
        return new BooleanStatisticsV1();
      case DOUBLE:
        return new DoubleStatisticsV1();
      case FLOAT:
        return new FloatStatisticsV1();
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public static StatisticsV1 deserialize(InputStream inputStream, TSDataType dataType)
      throws IOException {
    StatisticsV1<?> statistics = getStatsByType(dataType);
    statistics.deserialize(inputStream);
    return statistics;
  }

  public static StatisticsV1 deserialize(ByteBuffer buffer, TSDataType dataType) throws IOException {
    StatisticsV1<?> statistics = getStatsByType(dataType);
    statistics.deserialize(buffer);
    return statistics;
  }

  /**
   * For upgrading 0.9.x/v1 -> 0.10/v2
   */
  public static Statistics upgradeOldStatistics(StatisticsV1<?> oldstatistics,
      TSDataType dataType, int numOfValues, long maxTimestamp, long minTimestamp) {
    Statistics<?> statistics = Statistics.getStatsByType(dataType);
    statistics.setStartTime(minTimestamp);
    statistics.setEndTime(maxTimestamp);
    statistics.setCount(numOfValues);
    statistics.setEmpty(false);
    switch (dataType) {
      case INT32:
        ((IntegerStatistics) statistics)
        .initializeStats(((IntegerStatisticsV1) oldstatistics).getMin(),
            ((IntegerStatisticsV1) oldstatistics).getMax(),
            ((IntegerStatisticsV1) oldstatistics).getFirst(),
            ((IntegerStatisticsV1) oldstatistics).getLast(),
            ((IntegerStatisticsV1) oldstatistics).getSum());
        break;
      case INT64:
        ((LongStatistics) statistics)
        .initializeStats(((LongStatisticsV1) oldstatistics).getMin(),
            ((LongStatisticsV1) oldstatistics).getMax(),
            ((LongStatisticsV1) oldstatistics).getFirst(),
            ((LongStatisticsV1) oldstatistics).getLast(),
            ((LongStatisticsV1) oldstatistics).getSum());
        break;
      case TEXT:
        ((BinaryStatistics) statistics)
        .initializeStats(((BinaryStatisticsV1) oldstatistics).getFirst(),
            ((BinaryStatisticsV1) oldstatistics).getLast());
        break;
      case BOOLEAN:
        ((BooleanStatistics) statistics)
        .initializeStats(((BooleanStatisticsV1) oldstatistics).getFirst(),
            ((BooleanStatisticsV1) oldstatistics).getLast());
        break;
      case DOUBLE:
        ((DoubleStatistics) statistics)
        .initializeStats(((DoubleStatisticsV1) oldstatistics).getMin(),
            ((DoubleStatisticsV1) oldstatistics).getMax(),
            ((DoubleStatisticsV1) oldstatistics).getFirst(),
            ((DoubleStatisticsV1) oldstatistics).getLast(),
            ((DoubleStatisticsV1) oldstatistics).getSum());
        break;
      case FLOAT:
        ((FloatStatistics) statistics)
        .initializeStats(((FloatStatisticsV1) oldstatistics).getMin(),
            ((FloatStatisticsV1) oldstatistics).getMax(),
            ((FloatStatisticsV1) oldstatistics).getFirst(),
            ((FloatStatisticsV1) oldstatistics).getLast(),
            ((FloatStatisticsV1) oldstatistics).getSum());
        break;
      default:
        throw new UnknownColumnTypeException(statistics.getType()
            .toString());
    }
    return statistics;
  }

  /**
   * For upgrading 0.9.x/v1 -> 0.10.x/v2
   */
  public static Statistics constructStatisticsFromOldChunkMetadata(ChunkMetadataV1 oldChunkMetadata) {
    Statistics<?> statistics = Statistics.getStatsByType(oldChunkMetadata.getTsDataType());
    statistics.setStartTime(oldChunkMetadata.getStartTime());
    statistics.setEndTime(oldChunkMetadata.getEndTime());
    statistics.setCount(oldChunkMetadata.getNumOfPoints());
    statistics.setEmpty(false);
    TsDigestV1 tsDigest = oldChunkMetadata.getDigest();
    ByteBuffer[] buffers = tsDigest.getStatistics();
    switch (statistics.getType()) {
      case INT32:
        ((IntegerStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readInt(buffers[0]),
            ReadWriteIOUtils.readInt(buffers[1]),
            ReadWriteIOUtils.readInt(buffers[2]),
            ReadWriteIOUtils.readInt(buffers[3]),
            ReadWriteIOUtils.readDouble(buffers[4]));
        break;
      case INT64:
        ((LongStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readLong(buffers[0]),
            ReadWriteIOUtils.readLong(buffers[1]),
            ReadWriteIOUtils.readLong(buffers[2]),
            ReadWriteIOUtils.readLong(buffers[3]),
            ReadWriteIOUtils.readDouble(buffers[4]));
        break;
      case TEXT:
        ((BinaryStatistics) statistics)
        .initializeStats(new Binary(buffers[2].array()),
            new Binary(buffers[3].array()));
        break;
      case BOOLEAN:
        ((BooleanStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readBool(buffers[2]),
            ReadWriteIOUtils.readBool(buffers[3]));
        break;
      case DOUBLE:
        ((DoubleStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readDouble(buffers[0]),
            ReadWriteIOUtils.readDouble(buffers[1]),
            ReadWriteIOUtils.readDouble(buffers[2]),
            ReadWriteIOUtils.readDouble(buffers[3]),
            ReadWriteIOUtils.readDouble(buffers[4]));
        break;
      case FLOAT:
        ((FloatStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readFloat(buffers[0]),
            ReadWriteIOUtils.readFloat(buffers[1]),
            ReadWriteIOUtils.readFloat(buffers[2]),
            ReadWriteIOUtils.readFloat(buffers[3]),
            ReadWriteIOUtils.readDouble(buffers[4]));
        break;
      default:
        throw new UnknownColumnTypeException(statistics.getType()
            .toString());
    }
    return statistics;
  }

  public abstract T getMin();

  public abstract T getMax();

  public abstract T getFirst();

  public abstract T getLast();

  public abstract double getSum();

  /**
   * read data from the inputStream.
   */
  abstract void deserialize(InputStream inputStream) throws IOException;

  abstract void deserialize(ByteBuffer byteBuffer) throws IOException;

}
