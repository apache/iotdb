package org.apache.iotdb.pipe.api.customizer.plugin;

import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.config.ProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeValidator;
import org.apache.iotdb.pipe.api.event.DeletionEvent;
import org.apache.iotdb.pipe.api.event.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.TsFileInsertionEvent;

public interface PipeProcessor extends AutoCloseable {

  /**
   * This method is mainly used to validate {@link PipeParameters} and it is executed before {@link
   * PipeProcessor#beforeStart(PipeParameters, ProcessorRuntimeConfiguration)} is called.
   *
   * @param validator the validator used to validate {@link PipeParameters}
   * @throws Exception if any parameter is not valid
   */
  void validate(PipeValidator validator) throws Exception;

  /**
   * This method is mainly used to customize PipeProcessor. In this method, the user can do the
   * following things:
   *
   * <ul>
   *   <li>Use PipeParameters to parse key-value pair attributes entered by the user.
   *   <li>Set the running configurations in ProcessorRuntimeConfiguration.
   * </ul>
   *
   * <p>This method is called after the PipeProcessor is instantiated and before the beginning of
   * the data processing.
   *
   * @param params used to parse the input parameters entered by the user
   * @param configs used to set the required properties of the running PipeProcessor
   * @throws Exception the user can throw errors if necessary
   */
  void beforeStart(PipeParameters params, ProcessorRuntimeConfiguration configs) throws Exception;

  /**
   * This method is called to process the TabletInsertionEvent.
   *
   * @param te the insertion event of Tablet
   * @param ec used to collect output data events
   * @throws Exception the user can throw errors if necessary
   */
  void process(TabletInsertionEvent te, EventCollector ec) throws Exception;

  /**
   * This method is called to process the TsFileInsertionEvent.
   *
   * @param te the insertion event of TsFile
   * @param ec used to collect output data events
   * @throws Exception the user can throw errors if necessary
   */
  void process(TsFileInsertionEvent te, EventCollector ec) throws Exception;

  /**
   * This method is called to process the DeletionEvent.
   *
   * @param de the event of Deletion
   * @param ec used to collect output data events
   * @throws Exception the user can throw errors if necessary
   */
  void process(DeletionEvent de, EventCollector ec) throws Exception;
}
