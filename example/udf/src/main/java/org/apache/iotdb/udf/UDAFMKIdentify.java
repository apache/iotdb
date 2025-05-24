package org.apache.iotdb.udf;

import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.customizer.analysis.AggregateFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;
import org.apache.iotdb.udf.table.MatchingKeyState;

import org.apache.tsfile.utils.Binary;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

public class UDAFMKIdentify implements AggregateFunction {
  int label;
  int length;
  double min_confidence;
  double min_support;
  String a;
  int l = 0;

  private FunctionArguments arguments;

  public AggregateFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    int num = arguments.getArgumentsSize();
    if (num < 5) {
      throw new UDFArgumentNotValidException("At least 2 columns and 3 parameters are required.");
    }

    this.arguments = arguments;
    Type thirdLastType = arguments.getDataType(num - 3);
    Type secondLastType = arguments.getDataType(num - 2);
    Type lastType = arguments.getDataType(num - 1);

    if (thirdLastType != Type.INT32) {
      throw new UDFArgumentNotValidException(
          String.format(
              "The third last parameter must be of type INT, but found: %s", thirdLastType));
    }

    if (secondLastType != Type.DOUBLE) {
      throw new UDFArgumentNotValidException(
          String.format(
              "The second last parameter must be of type DOUBLE, but found: %s", secondLastType));
    }

    if (lastType != Type.DOUBLE) {
      throw new UDFArgumentNotValidException(
          String.format("The last parameter must be of type DOUBLE, but found: %s", lastType));
    }

    return new AggregateFunctionAnalysis.Builder()
        .outputDataType(Type.TEXT)
        .removable(true)
        .build();
  }

  @Override
  public State createState() {
    return new MatchingKeyState();
  }

  @Override
  public void addInput(State state, Record input) {
    MatchingKeyState mkState = (MatchingKeyState) state;
    int num = input.size();
    length = num - 3;
    label = input.getInt(length) - 1;
    min_support = input.getDouble(length + 1);
    min_confidence = input.getDouble(length + 2);
    mkState.init(label, min_support, min_confidence);
    length = length - 1;
    mkState.setColumnLength(length);
    long time = input.getLong(0);
    String[] fullTuple = new String[length];
    for (int i = 0; i < length; i++) {
      Type col0Type = arguments.getDataType(i + 1);
      String valueAsString;
      switch (col0Type) {
        case TEXT:
        case STRING:
          valueAsString = input.getString(i + 1);
          break;
        case INT32:
          valueAsString = Integer.toString(input.getInt(i + 1));
          break;
        case INT64:
          valueAsString = Long.toString(input.getLong(i + 1));
          break;
        case FLOAT:
          valueAsString = Float.toString(input.getFloat(i + 1));
          break;
        case DOUBLE:
          valueAsString = Double.toString(input.getDouble(i + 1));
          break;
        case BOOLEAN:
          valueAsString = Boolean.toString(input.getBoolean(i + 1));
          break;
        default:
          throw new RuntimeException("Unsupported data type for column 0: " + col0Type);
      }
      fullTuple[i] = valueAsString;
    }
    mkState.add(l, time, fullTuple);
    l++;
  }

  @Override
  public void combineState(State state, State rhs) {
    ((MatchingKeyState) state).merge((MatchingKeyState) rhs);
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    MatchingKeyState mkState = (MatchingKeyState) state;

    MatchingKeyState.Candidate psi = mkState.computeAllPairs();
    Set<MatchingKeyState.Candidate> Psi = mkState.MCG(psi, psi.getNegativeSet());
    Set<MatchingKeyState.Candidate> Psi0 = mkState.GAP(Psi);
    StringBuilder sb = new StringBuilder();

    int mkIndex = 1;
    int size = Psi0.size();
    if (size == 0) {
      String output = "Null";
      resultValue.setBinary(new Binary(output, Charset.defaultCharset()));
    } else {
      int index = 0;
      for (MatchingKeyState.Candidate c : Psi0) {
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
      resultValue.setBinary(new Binary(output, Charset.defaultCharset()));
    }
  }
}
