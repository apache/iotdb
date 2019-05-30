package org.apache.iotdb.db.tools;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.thrift.EncodingUtils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Created by wangyihan on 2019/5/29 10:50 PM.
 * E-mail address is yihanwang22@163.com.
 * Copyright © 2017 wangyihan. All Rights Reserved.
 *
 * @author wangyihan
 */
public class WatermarkEncoder {

  private String secretKey;
  private String bitString;
  private int markRate = 2;
  private int groupNumber = 20;
  private int maxBitPosition = 5;
  private int minBitPosition = 0;

  public WatermarkEncoder(String secretKey, String bitString) {
    this.secretKey = secretKey;
    this.bitString = bitString;
  }

  public void setMarkRate(int markRate) {
    this.markRate = markRate;
  }

  public void setGroupNumber(int groupNumber) {
    this.groupNumber = groupNumber;
  }

  public void setMaxBitPosition(int maxBitPosition) {
    this.maxBitPosition = maxBitPosition;
  }

  public void setMinBitPosition(int minBitPosition) {
    this.minBitPosition = minBitPosition;
  }

  private static int hashMod(String val, Integer base) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      throw new RuntimeException("ERROR: Cannot find MD5 algorithm!");
    }
    md.update(val.getBytes());
    BigInteger resultInteger = new BigInteger(1, md.digest());
    return resultInteger.mod(new BigInteger(base.toString())).intValue();
  }

  private boolean getMarkFlag(long timestamp) {
    return hashMod(String.format("%d%s", timestamp, secretKey), markRate) == 0;
  }

  private int getGroupId(long timestamp) {
    return hashMod(String.format("%s%d%s", secretKey, timestamp, secretKey), groupNumber);
  }

  private int getBitPosition(long timestamp) {
    if (maxBitPosition <= minBitPosition) {
      throw new RuntimeException("Error: minBitPosition is bigger than maxBitPosition");
    }
    int range = maxBitPosition - minBitPosition;
    return minBitPosition + hashMod(String.format("%d%s", timestamp, secretKey), range);
  }

  private boolean getBitValue(long timestamp) {
    int groupId = getGroupId(timestamp);
    int bitIndex = groupId % bitString.length();
    return bitString.charAt(bitIndex) == '1';
  }

  private int encodeInt(int value, long timestamp) {
    int targetBitPosition = getBitPosition(timestamp);
    boolean targetBitValue = getBitValue(timestamp);
    return EncodingUtils.setBit(value, targetBitPosition, targetBitValue);
  }

  private long encodeLong(long value, long timestamp) {
    int targetBitPosition = getBitPosition(timestamp);
    boolean targetBitValue = getBitValue(timestamp);
    return EncodingUtils.setBit(value, targetBitPosition, targetBitValue);
  }

  private float encodeFloat(float value, long timestamp) {
    int integerPart = (int) value;
    float decimalPart = value - integerPart;
    return encodeInt(integerPart, timestamp) + decimalPart;
  }

  private double encodeDouble(double value, long timestamp) {
    int integerPart = (int) value;
    double decimalPart = value - integerPart;
    return encodeInt(integerPart, timestamp) + decimalPart;
  }

  public RowRecord encodeRecord(RowRecord record) {
    long timestamp = record.getTimestamp();
    if (!getMarkFlag(timestamp)) {
      return record;
    }
    List<Field> fields = record.getFields();
    for (Field field : fields) {
      TSDataType dataType = field.getDataType();
      switch (dataType) {
        case INT32:
          int originIntValue = field.getIntV();
          field.setIntV(encodeInt(originIntValue, timestamp));
          break;
        case INT64:
          long originLongValue = field.getLongV();
          field.setLongV(encodeLong(originLongValue, timestamp));
          break;
        case FLOAT:
          float originFloatValue = field.getFloatV();
          field.setFloatV(encodeFloat(originFloatValue, timestamp));
          break;
        case DOUBLE:
          double originDoubleValue = field.getDoubleV();
          field.setDoubleV(encodeDouble(originDoubleValue, timestamp));
          break;
        default:
      }
    }
    return record;
  }
}
