package org.apache.iotdb.pipe.api.customizer.plugin;

import org.apache.iotdb.pipe.api.customizer.config.ConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeValidator;
import org.apache.iotdb.pipe.api.event.DeletionEvent;
import org.apache.iotdb.pipe.api.event.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.TsFileInsertionEvent;

public interface PipeConnector extends AutoCloseable {

  /**
   * This method is mainly used to validate {@link PipeParameters} and it is executed before {@link
   * PipeConnector#beforeStart(PipeParameters, ConnectorRuntimeConfiguration)} is called.
   *
   * @param validator the validator used to validate {@link PipeParameters}
   * @throws Exception if any parameter is not valid
   */
  void validate(PipeValidator validator) throws Exception;

  /**
   * This method is mainly used to customize PipeConnector. In this method, the user can do the
   * following things:
   *
   * <ul>
   *   <li>Use PipeParameters to parse key-value pair attributes entered by the user.
   *   <li>Set the running configurations in ConnectorRuntimeConfiguration.
   * </ul>
   *
   * <p>This method is called after the PipeConnector is instantiated and before the beginning of
   * the server connection.
   *
   * @param params used to parse the input parameters entered by the user
   * @param config used to set the required properties of the running PipeConnector
   * @throws Exception the user can throw errors if necessary
   */
  void beforeStart(PipeParameters params, ConnectorRuntimeConfiguration config) throws Exception;

  /**
   * PipeConnector use PipeParameters which set in the method {@link
   * PipeConnector#beforeStart(PipeParameters, ConnectorRuntimeConfiguration)} to establish the
   * handshake connection between servers.
   *
   * @throws Exception the user can throw errors if necessary
   */
  void handshake() throws Exception;

  /**
   * This method is called to check whether the PipeConnector is connected to the servers.
   *
   * @throws Exception the user can throw errors if necessary
   */
  void heartbeat() throws Exception;

  /**
   * This method is called to transfer the TabletInsertionEvent.
   *
   * @param te the insertion event of Tablet
   * @throws Exception the user can throw errors if necessary
   */
  void transfer(TabletInsertionEvent te) throws Exception;

  /**
   * This method is called to transfer the TsFileInsertionEvent.
   *
   * @param te the insertion event of TsFile
   * @throws Exception the user can throw errors if necessary
   */
  void transfer(TsFileInsertionEvent te) throws Exception;

  /**
   * This method is called to transfer the DeletionEvent.
   *
   * @param de the insertion event of Deletion
   * @throws Exception the user can throw errors if necessary
   */
  void transfer(DeletionEvent de) throws Exception;
}
