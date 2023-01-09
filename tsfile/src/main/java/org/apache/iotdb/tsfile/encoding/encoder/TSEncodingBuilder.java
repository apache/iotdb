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
package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Each subclass of TSEncodingBuilder responds a enumerate value in {@linkplain TSEncoding
 * TSEncoding}, which stores several configuration related to responding encoding type to generate
 * {@linkplain Encoder Encoder} instance.<br>
 * Each TSEncoding has a responding TSEncodingBuilder. The design referring to visit pattern
 * provides same outer interface for different TSEncodings and gets rid of the duplicate switch-case
 * code.
 */
public abstract class TSEncodingBuilder {

  private static final Logger logger = LoggerFactory.getLogger(TSEncodingBuilder.class);
  protected final TSFileConfig conf;

  public TSEncodingBuilder() {
    this.conf = TSFileDescriptor.getInstance().getConfig();
  }

  /**
   * return responding TSEncodingBuilder from a TSEncoding.
   *
   * @param type - given encoding type
   * @return - responding TSEncodingBuilder
   */
  public static TSEncodingBuilder getEncodingBuilder(TSEncoding type) {
    switch (type) {
      case PLAIN:
        return new Plain();
      case RLE:
        return new Rle();
      case TS_2DIFF:
        return new Ts2Diff();
      case GORILLA_V1:
        return new GorillaV1();
      case REGULAR:
        return new Regular();
      case GORILLA:
        return new GorillaV2();
      case DICTIONARY:
        return new Dictionary();
      case FREQ:
        return new Freq();
      case ZIGZAG:
        return new Zigzag();
      case CHIMP:
        return new Chimp();
      default:
        throw new UnsupportedOperationException(type.toString());
    }
  }

  /**
   * return a thread safe series's encoder with different types and parameters according to its
   * measurement id and data type.
   *
   * @param type - given data type
   * @return - return a {@linkplain Encoder Encoder}
   */
  public abstract Encoder getEncoder(TSDataType type);

  /**
   * for TSEncoding, JSON is a kind of type for initialization. {@code InitFromJsonObject} gets
   * values from JSON object which will be used latter.<br>
   * if this type has extra parameters to construct, override it.
   *
   * @param props - properties of encoding
   */
  public abstract void initFromProps(Map<String, String> props);

  @Override
  public String toString() {
    return "";
  }

  /** for all TSDataType. */
  public static class Plain extends TSEncodingBuilder {

    private int maxStringLength = TSFileDescriptor.getInstance().getConfig().getMaxStringLength();

    @Override
    public Encoder getEncoder(TSDataType type) {
      return new PlainEncoder(type, maxStringLength);
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // set max error from initialized map or default value if not set
      if (props == null || !props.containsKey(Encoder.MAX_STRING_LENGTH)) {
        maxStringLength = TSFileDescriptor.getInstance().getConfig().getMaxStringLength();
      } else {
        maxStringLength = Integer.valueOf(props.get(Encoder.MAX_STRING_LENGTH));
        if (maxStringLength < 0) {
          maxStringLength = TSFileDescriptor.getInstance().getConfig().getMaxStringLength();
          logger.warn(
              "cannot set max string length to negative value, replaced with default value:{}",
              maxStringLength);
        }
      }
    }
  }

  /** for INT32, INT64, FLOAT, DOUBLE. */
  public static class Freq extends TSEncodingBuilder {

    private double snr = TSFileDescriptor.getInstance().getConfig().getFreqEncodingSNR();
    private int blockSize = TSFileDescriptor.getInstance().getConfig().getFreqEncodingBlockSize();

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
          return new FreqEncoder(blockSize, snr);
        default:
          throw new UnSupportedDataTypeException("FREQ doesn't support data type: " + type);
      }
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // set SNR from initialized map or default value if not set
      if (props == null || !props.containsKey(FreqEncoder.FREQ_ENCODING_SNR)) {
        snr = TSFileDescriptor.getInstance().getConfig().getFreqEncodingSNR();
      } else {
        try {
          snr = Double.parseDouble(props.get(FreqEncoder.FREQ_ENCODING_SNR));
        } catch (NumberFormatException e) {
          logger.warn(
              "The format of FREQ encoding SNR {} is not correct."
                  + " Using default FREQ encoding SNR.",
              props.get(FreqEncoder.FREQ_ENCODING_SNR));
        }
        if (snr < 0) {
          snr = TSFileDescriptor.getInstance().getConfig().getFreqEncodingSNR();
          logger.warn(
              "cannot set FREQ encoding SNR to negative value, replaced with default value:{}",
              snr);
        }
      }
      // set block size from initialized map or default value if not set
      if (props == null || !props.containsKey(FreqEncoder.FREQ_ENCODING_BLOCK_SIZE)) {
        blockSize = TSFileDescriptor.getInstance().getConfig().getFreqEncodingBlockSize();
      } else {
        try {
          blockSize = Integer.parseInt(props.get(FreqEncoder.FREQ_ENCODING_BLOCK_SIZE));
        } catch (NumberFormatException e) {
          logger.warn(
              "The format of FREQ encoding block size {} is not correct."
                  + " Using default FREQ encoding block size.",
              props.get(FreqEncoder.FREQ_ENCODING_BLOCK_SIZE));
        }
        if (blockSize < 0) {
          blockSize = TSFileDescriptor.getInstance().getConfig().getFreqEncodingBlockSize();
          logger.warn(
              "cannot set FREQ encoding block size to negative value, replaced with default value:{}",
              blockSize);
        }
      }
    }
  }

  /** for ENUMS, INT32, BOOLEAN, INT64, FLOAT, DOUBLE. */
  public static class Rle extends TSEncodingBuilder {

    private int maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case INT32:
        case BOOLEAN:
          return new IntRleEncoder();
        case INT64:
          return new LongRleEncoder();
        case FLOAT:
        case DOUBLE:
          return new FloatEncoder(TSEncoding.RLE, type, maxPointNumber);
        default:
          throw new UnSupportedDataTypeException("RLE doesn't support data type: " + type);
      }
    }

    /**
     * RLE could specify <b>max_point_number</b> in given JSON Object, which means the maximum
     * decimal digits for float or double data.
     */
    @Override
    public void initFromProps(Map<String, String> props) {
      // set max error from initialized map or default value if not set
      if (props == null || !props.containsKey(Encoder.MAX_POINT_NUMBER)) {
        maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
      } else {
        try {
          this.maxPointNumber = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
        } catch (NumberFormatException e) {
          logger.warn(
              "The format of max point number {} is not correct."
                  + " Using default float precision.",
              props.get(Encoder.MAX_POINT_NUMBER));
        }
        if (maxPointNumber < 0) {
          maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
          logger.warn(
              "cannot set max point number to negative value, replaced with default value:{}",
              maxPointNumber);
        }
      }
    }

    @Override
    public String toString() {
      return JsonFormatConstant.MAX_POINT_NUMBER + ":" + maxPointNumber;
    }
  }

  /** for INT32, INT64, FLOAT, DOUBLE. */
  public static class Ts2Diff extends TSEncodingBuilder {

    private int maxPointNumber = 0;

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case INT32:
          return new DeltaBinaryEncoder.IntDeltaEncoder();
        case INT64:
          return new DeltaBinaryEncoder.LongDeltaEncoder();
        case FLOAT:
        case DOUBLE:
          return new FloatEncoder(TSEncoding.TS_2DIFF, type, maxPointNumber);
        default:
          throw new UnSupportedDataTypeException("TS_2DIFF doesn't support data type: " + type);
      }
    }

    @Override
    /**
     * TS_2DIFF could specify <b>max_point_number</b> in given JSON Object, which means the maximum
     * decimal digits for float or double data.
     */
    public void initFromProps(Map<String, String> props) {
      // set max error from initialized map or default value if not set
      if (props == null || !props.containsKey(Encoder.MAX_POINT_NUMBER)) {
        maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
      } else {
        try {
          this.maxPointNumber = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
        } catch (NumberFormatException e) {
          logger.warn(
              "The format of max point number {} is not correct."
                  + " Using default float precision.",
              props.get(Encoder.MAX_POINT_NUMBER));
        }
        if (maxPointNumber < 0) {
          maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
          logger.warn(
              "cannot set max point number to negative value, replaced with default value:{}",
              maxPointNumber);
        }
      }
    }

    @Override
    public String toString() {
      return JsonFormatConstant.MAX_POINT_NUMBER + ":" + maxPointNumber;
    }
  }

  /** for FLOAT, DOUBLE. */
  public static class GorillaV1 extends TSEncodingBuilder {

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case FLOAT:
          return new SinglePrecisionEncoderV1();
        case DOUBLE:
          return new DoublePrecisionEncoderV1();
        default:
          throw new UnSupportedDataTypeException("GORILLA_V1 doesn't support data type: " + type);
      }
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // allowed do nothing
    }
  }

  /** for INT32, INT64 */
  public static class Regular extends TSEncodingBuilder {

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case INT32:
          return new RegularDataEncoder.IntRegularEncoder();
        case INT64:
          return new RegularDataEncoder.LongRegularEncoder();
        default:
          throw new UnSupportedDataTypeException("REGULAR doesn't support data type: " + type);
      }
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // allowed do nothing
    }
  }

  /** for FLOAT, DOUBLE, INT, LONG. */
  public static class GorillaV2 extends TSEncodingBuilder {

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case FLOAT:
          return new SinglePrecisionEncoderV2();
        case DOUBLE:
          return new DoublePrecisionEncoderV2();
        case INT32:
          return new IntGorillaEncoder();
        case INT64:
          return new LongGorillaEncoder();
        default:
          throw new UnSupportedDataTypeException("GORILLA doesn't support data type: " + type);
      }
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // allowed do nothing
    }
  }

  public static class Dictionary extends TSEncodingBuilder {

    @Override
    public Encoder getEncoder(TSDataType type) {
      if (type == TSDataType.TEXT) {
        return new DictionaryEncoder();
      }
      throw new UnSupportedDataTypeException("DICTIONARY doesn't support data type: " + type);
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // do nothing
    }
  }

  public static class Zigzag extends TSEncodingBuilder {

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case INT32:
          return new IntZigzagEncoder();
        case INT64:
          return new LongZigzagEncoder();
        default:
          throw new UnSupportedDataTypeException("ZIGZAG doesn't support data type: " + type);
      }
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // do nothing
    }
  }

  /** for FLOAT, DOUBLE, INT, LONG. */
  public static class Chimp extends TSEncodingBuilder {

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case FLOAT:
          return new SinglePrecisionChimpEncoder();
        case DOUBLE:
          return new DoublePrecisionChimpEncoder();
        case INT32:
          return new IntChimpEncoder();
        case INT64:
          return new LongChimpEncoder();
        default:
          throw new UnSupportedDataTypeException("CHIMP doesn't support data type: " + type);
      }
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // allowed do nothing
    }
  }
}
