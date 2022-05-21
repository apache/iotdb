package org.apache.iotdb.db.mpp.transformation.dag.transformer.ternary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;

import java.io.IOException;

public abstract class TernaryTransformer extends Transformer {
  protected final LayerPointReader firstPointReader;
  protected final LayerPointReader secondPointReader;
  protected final LayerPointReader thirdPointReader;

  public TernaryTransformer(
      LayerPointReader firstPointReader,
      LayerPointReader secondPointReader,
      LayerPointReader thirdPointReader) {
    this.firstPointReader = firstPointReader;
    this.secondPointReader = secondPointReader;
    this.thirdPointReader = thirdPointReader;
  }

  @Override
  public boolean isConstantPointReader() {
    return firstPointReader.isConstantPointReader()
        && secondPointReader.isConstantPointReader()
        && thirdPointReader.isConstantPointReader();
  }

  @Override
  protected boolean cacheValue() throws QueryProcessException, IOException {
    if (!firstPointReader.next() || !secondPointReader.next() || thirdPointReader.next()) {
      return false;
    }

    if (!cacheTime()) {
      return false;
    }

    if (firstPointReader.isCurrentNull()
        || secondPointReader.isCurrentNull()
        || thirdPointReader.isCurrentNull()) {
      currentNull = true;
    } else {
      transformAndCache();
    }

    firstPointReader.readyForNext();
    secondPointReader.readyForNext();
    thirdPointReader.readyForNext();
    return true;
  }

  protected abstract void transformAndCache() throws QueryProcessException, IOException;

  /**
   * finds the smallest, unconsumed, same timestamp that exists in {@code firstPointReader}, {@code
   * rightPointReader} and {@code thirdPointReader}and then caches the timestamp in {@code
   * cachedTime}.
   *
   * @return true if there has a timestamp that meets the requirements
   */
  private boolean cacheTime() throws IOException, QueryProcessException {
    boolean isFirstConstant = firstPointReader.isConstantPointReader();
    boolean isSecondConstant = secondPointReader.isConstantPointReader();
    boolean isThirdConstant = thirdPointReader.isConstantPointReader();
    long firstTime = isFirstConstant ? 0 : firstPointReader.currentTime();
    long secondTime = isSecondConstant ? 0 : secondPointReader.currentTime();
    long thirdTime = isThirdConstant ? 0 : secondPointReader.currentTime();

    while (firstTime != secondTime || firstTime != thirdTime) { // the logic is similar to MergeSort
      if (firstTime < secondTime) {
        if (isFirstConstant) {
          firstTime = secondTime;
        } else {
          firstPointReader.readyForNext();
          if (!firstPointReader.next()) {
            return false;
          }
          firstTime = firstPointReader.currentTime();
        }
      } else if (secondTime < thirdTime) {
        if (isSecondConstant) {
          secondTime = thirdTime;
        } else {
          secondPointReader.readyForNext();
          if (!secondPointReader.next()) {
            return false;
          }
          secondTime = secondPointReader.currentTime();
        }
      } else if (thirdTime < firstTime) {
        if (isThirdConstant) {
          firstTime = secondTime;
        } else {
          thirdPointReader.readyForNext();
          if (!thirdPointReader.next()) {
            return false;
          }
          thirdTime = secondPointReader.currentTime();
        }
      } else {
        cachedTime = firstTime;
        return true;
      }
    }

    if (firstTime
        != 0) // Value of firstTime, secondTime and thirdTime are same here, the value equals zero
      // represents three LayerPointerReaders are all ConstantPointReader.
      cachedTime = firstTime;
    return true;
  }

  protected static double castCurrentValueToDoubleOperand(LayerPointReader layerPointReader)
      throws IOException, QueryProcessException {
    switch (layerPointReader.getDataType()) {
      case INT32:
        return layerPointReader.currentInt();
      case INT64:
        return layerPointReader.currentLong();
      case FLOAT:
        return layerPointReader.currentFloat();
      case DOUBLE:
        return layerPointReader.currentDouble();
      default:
        throw new QueryProcessException(
            "Unsupported data type: " + layerPointReader.getDataType().toString());
    }
  }
}
