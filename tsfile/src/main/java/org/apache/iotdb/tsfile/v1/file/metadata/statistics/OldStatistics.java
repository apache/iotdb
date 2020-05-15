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
import org.apache.iotdb.tsfile.v1.file.metadata.OldChunkMetadata;
import org.apache.iotdb.tsfile.v1.file.metadata.TsDigest;

/**
 * This class is used for recording statistic information of each measurement in a delta file. While
 * writing processing, the processor records the digest information. Statistics includes maximum,
 * minimum and null value count up to version 0.0.1.<br> Each data type extends this Statistic as
 * super class.<br>
 *
 * @param <T> data type for Statistics
 */
public abstract class OldStatistics<T> {

  /**
   * static method providing statistic instance for respective data type.
   *
   * @param type - data type
   * @return Statistics
   */
  public static OldStatistics getStatsByType(TSDataType type) {
    switch (type) {
      case INT32:
        return new OldIntegerStatistics();
      case INT64:
        return new OldLongStatistics();
      case TEXT:
        return new OldBinaryStatistics();
      case BOOLEAN:
        return new OldBooleanStatistics();
      case DOUBLE:
        return new OldDoubleStatistics();
      case FLOAT:
        return new OldFloatStatistics();
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public static OldStatistics deserialize(InputStream inputStream, TSDataType dataType)
      throws IOException {
    OldStatistics<?> statistics = getStatsByType(dataType);
    statistics.deserialize(inputStream);
    return statistics;
  }

  public static OldStatistics deserialize(ByteBuffer buffer, TSDataType dataType) throws IOException {
    OldStatistics<?> statistics = getStatsByType(dataType);
    statistics.deserialize(buffer);
    return statistics;
  }
  
  /**
   * For upgrading 0.9.x/v1 -> 0.10/v2
   */
  public static Statistics upgradeOldStatistics(OldStatistics<?> oldstatistics, 
      TSDataType dataType, int numOfValues, long maxTimestamp, long minTimestamp) {
    Statistics<?> statistics = Statistics.getStatsByType(dataType);
    statistics.setStartTime(minTimestamp);
    statistics.setEndTime(maxTimestamp);
    statistics.setCount(numOfValues);
    statistics.setEmpty(false);
    switch (dataType) {
      case INT32:
        ((IntegerStatistics) statistics)
        .initializeStats(((OldIntegerStatistics) oldstatistics).getMin(), 
            ((OldIntegerStatistics) oldstatistics).getMax(), 
            ((OldIntegerStatistics) oldstatistics).getFirst(),
            ((OldIntegerStatistics) oldstatistics).getLast(),
            ((OldIntegerStatistics) oldstatistics).getSum());
        break;
      case INT64:
        ((LongStatistics) statistics)
        .initializeStats(((OldLongStatistics) oldstatistics).getMin(), 
            ((OldLongStatistics) oldstatistics).getMax(), 
            ((OldLongStatistics) oldstatistics).getFirst(),
            ((OldLongStatistics) oldstatistics).getLast(),
            ((OldLongStatistics) oldstatistics).getSum());
        break;
      case TEXT:
        ((BinaryStatistics) statistics)
        .initializeStats(((OldBinaryStatistics) oldstatistics).getFirst(),
            ((OldBinaryStatistics) oldstatistics).getLast());
        break;
      case BOOLEAN:
        ((BooleanStatistics) statistics)
        .initializeStats(((OldBooleanStatistics) oldstatistics).getFirst(),
            ((OldBooleanStatistics) oldstatistics).getLast());
        break;
      case DOUBLE:
        ((DoubleStatistics) statistics)
        .initializeStats(((OldDoubleStatistics) oldstatistics).getMin(), 
            ((OldDoubleStatistics) oldstatistics).getMax(), 
            ((OldDoubleStatistics) oldstatistics).getFirst(),
            ((OldDoubleStatistics) oldstatistics).getLast(),
            ((OldDoubleStatistics) oldstatistics).getSum());
        break;
      case FLOAT:
        ((FloatStatistics) statistics)
        .initializeStats(((OldFloatStatistics) oldstatistics).getMin(), 
            ((OldFloatStatistics) oldstatistics).getMax(), 
            ((OldFloatStatistics) oldstatistics).getFirst(),
            ((OldFloatStatistics) oldstatistics).getLast(),
            ((OldFloatStatistics) oldstatistics).getSum());
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
  public static Statistics constructStatisticsFromOldChunkMetadata(OldChunkMetadata oldChunkMetadata) {
    Statistics<?> statistics;
    statistics = Statistics.getStatsByType(oldChunkMetadata.getTsDataType());
    statistics.setStartTime(oldChunkMetadata.getStartTime());
    statistics.setEndTime(oldChunkMetadata.getEndTime());
    statistics.setCount(oldChunkMetadata.getNumOfPoints());
    statistics.setEmpty(false);
    TsDigest tsDigest = oldChunkMetadata.getDigest();
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
