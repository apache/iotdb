package org.apache.iotdb.db.mpp.transformation.dag.transformer.ternary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class BetweenTransformer extends TernaryTransformer {

  public BetweenTransformer(
      LayerPointReader firstPointReader,
      LayerPointReader secondPointReader,
      LayerPointReader thirdPointReader) {
    super(firstPointReader, secondPointReader, thirdPointReader);
  }

  protected boolean evaluate(double firstOperand, double secondOperand, double thirdOperand) {
    return Double.compare(firstOperand, secondOperand) >= 0
        && Double.compare(firstOperand, thirdOperand) <= 0;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    cachedBoolean =
        evaluate(
            castCurrentValueToDoubleOperand(firstPointReader),
            castCurrentValueToDoubleOperand(secondPointReader),
            castCurrentValueToDoubleOperand(thirdPointReader));
  }
}
