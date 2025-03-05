package org.apache.iotdb.commons.pipe.agent.plugin.builtin.extractor.external;

import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

@TreeModel
@TableModel
public class PipeExternalExtractor implements PipeExtractor {
  @Override
  public void validate(PipeParameterValidator validator) throws Exception {}

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {}

  @Override
  public void start() throws Exception {}

  @Override
  public Event supply() throws Exception {
    return null;
  }

  @Override
  public void close() throws Exception {}
}
