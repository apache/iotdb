package org.apache.iotdb.library.dmatch;

import org.apache.iotdb.library.dmatch.util.MatchingKey;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import java.util.List;
import java.util.Set;

public class UDAFMKIdentify implements UDTF {
  static final String MIN_CONFIDENCE_PARAM = "min_confidence";
  static final String MIN_SUPPORT_PARAM = "min_support";
  static final String LABEL_PARAM = "label";
  int label;
  int length;
  double min_confidence;
  double min_support;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    String labelStr = validator.getParameters().getStringOrDefault(LABEL_PARAM, null);
    if (labelStr == null) {
      throw new UDFParameterNotValidException(
          "The 'label' parameter is required and cannot be empty.");
    } else {
      label = Integer.parseInt(labelStr);
    }
    String minConfidenceStr =
        validator.getParameters().getStringOrDefault(MIN_CONFIDENCE_PARAM, "1.0").trim();
    try {
      min_confidence = Double.parseDouble(minConfidenceStr);
    } catch (NumberFormatException e) {
      throw new UDFParameterNotValidException(
          "Invalid value for 'min_confidence'. It must be a valid numeric value.");
    }

    String minSupportStr =
        validator.getParameters().getStringOrDefault(MIN_SUPPORT_PARAM, "0.5").trim();
    try {
      min_support = Double.parseDouble(minSupportStr);
    } catch (NumberFormatException e) {
      throw new UDFParameterNotValidException(
          "Invalid value for 'min_support'. It must be a valid numeric value.");
    }

    validator
        .validateInputSeriesNumber(2, Integer.MAX_VALUE)
        .validateRequiredAttribute(LABEL_PARAM)
        .validate(
            payload -> ((double) payload) >= 0.0 && ((double) payload) <= 1.0,
            "Invalid parameter: 'min_confidence' must be a double between 0 and 1.",
            min_confidence)
        .validate(
            payload -> ((double) payload) >= 0.0 && ((double) payload) <= 1.0,
            "Invalid parameter: 'min_support' must be a double between 0 and 1.",
            min_support);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new SlidingSizeWindowAccessStrategy(1000));
    configurations.setOutputDataType(Type.TEXT);
    String labelStr = parameters.getString(LABEL_PARAM).trim();
    label = Integer.parseInt(labelStr);
    String minConfidenceStr = parameters.getStringOrDefault(MIN_CONFIDENCE_PARAM, "1.0").trim();
    min_confidence = Double.parseDouble(minConfidenceStr);
    String minSupportStr = parameters.getStringOrDefault(MIN_SUPPORT_PARAM, "1.0").trim();
    min_support = Double.parseDouble(minSupportStr);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    MatchingKey mk = new MatchingKey(label, min_support, min_confidence);
    int rowCount = rowWindow.windowSize();
    long nowTime = rowWindow.getRow(0).getTime();
    length = rowWindow.getRow(0).size();
    mk.setColumnLength(length);
    for (int i = 0; i < rowCount; i++) {
      Row row = rowWindow.getRow(i);
      String[] fullTuple = new String[row.size()];
      long time = row.getTime();
      for (int j = 0; j < row.size(); j++) {
        Type type = row.getDataType(j);
        String valueStr;
        try {
          switch (type) {
            case BOOLEAN:
              valueStr = String.valueOf(row.getBoolean(j));
              break;
            case INT32:
              valueStr = String.valueOf(row.getInt(j));
              break;
            case INT64:
              valueStr = String.valueOf(row.getLong(j));
              break;
            case FLOAT:
              valueStr = String.valueOf(row.getFloat(j));
              break;
            case DOUBLE:
              valueStr = String.valueOf(row.getDouble(j));
              break;
            case TEXT:
              try {
                valueStr = row.getBinary(j).getStringValue();
              } catch (ClassCastException e) {
                System.err.println(
                    "Warning: Expected TEXT, but actual value is not Binary at column " + j);
                try {
                  valueStr = String.valueOf(row.getLong(j));
                } catch (Exception ex) {
                  valueStr = "UNSUPPORTED";
                }
              }
              break;
            default:
              valueStr = "UNSUPPORTED";
              System.err.println("Unsupported data type at column " + j + ": " + type);
          }
        } catch (Exception e) {
          System.err.println(
              "Error reading column " + j + ": type=" + type + ", error=" + e.getMessage());
          valueStr = "ERROR";
        }
        fullTuple[j] = valueStr;
      }
      mk.add(i, time, fullTuple);
    }
    StringBuilder sb = new StringBuilder();
    MatchingKey.Candidate psi = mk.computeAllPairs();
    Set<MatchingKey.Candidate> Psi = mk.MCG(psi, psi.getNegativeSet());
    Set<MatchingKey.Candidate> Psi0 = mk.GAP(Psi);
    int mkIndex = 1;
    int size = Psi0.size();
    if (size == 0) {
      String output =
          "No matching keys meet the specified minimum support and confidence thresholds.";
      collector.putString(nowTime, output);
    } else {
      int index = 0;
      for (MatchingKey.Candidate c : Psi0) {
        sb.append("MK").append(mkIndex).append(":{");
        List<int[]> restrictions = c.getDistanceRestrictions();
        for (int i = 0; i < restrictions.size(); i++) {
          int[] r = restrictions.get(i);
          sb.append("[").append(r[0]).append(",").append(r[1]).append("]");
          if (i != restrictions.size() - 1) {
            sb.append(",");
          }
        }
        if (index != size - 1) {
          sb.append("}\n");
        } else {
          sb.append("}");
        }
        mkIndex++;
        index++;
      }

      String output = sb.toString();
      collector.putString(nowTime, output);
    }
  }
}
