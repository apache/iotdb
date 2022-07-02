package org.apache.iotdb.udf.api.customizer.strategy;

import org.apache.iotdb.udf.api.MappableUDTF;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

/**
 * Used in {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
 * <p>
 * When the access strategy of a UDTF is set to an instance of this class, the method {@link
 * MappableUDTF#transform(Row)} of the MappableUDTF will be called to transform the original data.
 * You need to override the method in your own MappableUDTF class.
 * <p>
 * Each call of the method {@link MappableUDTF#transform(Row)} processes only one row
 * (aligned by time) of the original data and can generate any number of data points.
 * <p>
 * Sample code:
 * <pre>{@code
 * @Override
 * public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
 *   configurations
 *       .setOutputDataType(TSDataType.INT64)
 *       .setAccessStrategy(new MappableRowByRowAccessStrategy());
 * }</pre>
 *
 * accessStrategy can be set to MappableRowByRowAccessStrategy if and only if udtf classes inherit from MappableUDTF
 *
 * @see UDTF
 * @see UDTFConfigurations
 */
public class MappableRowByRowAccessStrategy implements AccessStrategy {
  @Override
  public void check() {
    // nothing needs to check
  }

  @Override
  public AccessStrategyType getAccessStrategyType() {
    return AccessStrategyType.MAPPABLE_ROW_BY_ROW;
  }
}
