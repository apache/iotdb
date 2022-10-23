package org.apache.iotdb.db.service.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;

public class TThreadPoolServerWithContext extends TThreadPoolServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TThreadPoolServerWithContext.class);

  public TThreadPoolServerWithContext(Args args) {
    super(args);
  }

  @Override
  protected void execute() {
    while (!stopped_) {
      try {
        TTransport client = serverTransport_.accept();
        try {
          getExecutorService().execute(new WorkerProcess(client));
        } catch (RejectedExecutionException ree) {
          if (!stopped_) {
            LOGGER.warn(
                "ThreadPool is saturated with incoming requests. Closing latest connection.");
          }
          client.close();
        }
      } catch (TTransportException ttx) {
        if (!stopped_) {
          LOGGER.warn("Transport error occurred during acceptance of message", ttx);
        }
      }
    }
  }

  // The following codes are copied from TThreadPoolServer.WorkerProcess
  // and we add additional processing for connectionContext

  private class WorkerProcess implements Runnable {

    /** Client that this services. */
    private TTransport client_;

    /**
     * Default constructor.
     *
     * @param client Transport to process
     */
    private WorkerProcess(TTransport client) {
      client_ = client;
    }

    /** Loops on processing a client forever */
    public void run() {
      TProcessor processor = null;
      TTransport inputTransport = null;
      TTransport outputTransport = null;
      TProtocol inputProtocol = null;
      TProtocol outputProtocol = null;

      Optional<TServerEventHandler> eventHandler = Optional.empty();
      ServerContext connectionContext = null;

      try {
        processor = processorFactory_.getProcessor(client_);
        inputTransport = inputTransportFactory_.getTransport(client_);
        outputTransport = outputTransportFactory_.getTransport(client_);
        inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
        outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);

        eventHandler = Optional.ofNullable(getEventHandler());

        if (eventHandler.isPresent()) {
          connectionContext = eventHandler.get().createContext(inputProtocol, outputProtocol);
          LOGGER.error("测试一下效果。、。。。。");
        }

        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            LOGGER.debug("WorkerProcess requested to shutdown");
            break;
          }
          if (eventHandler.isPresent()) {
            eventHandler.get().processContext(connectionContext, inputTransport, outputTransport);
          }
          // This process cannot be interrupted by Interrupting the Thread. This
          // will return once a message has been processed or the socket timeout
          // has elapsed, at which point it will return and check the interrupt
          // state of the thread.
          processor.process(inputProtocol, outputProtocol);
        }
      } catch (Exception x) {
        LOGGER.debug("Error processing request", x);

        // We'll usually receive RuntimeException types here
        // Need to unwrap to ascertain real causing exception before we choose to ignore
        // Ignore err-logging all transport-level/type exceptions
        if (!isIgnorableException(x)) {
          // Log the exception at error level and continue
          LOGGER.error(
              (x instanceof TException ? "Thrift " : "")
                  + "Error occurred during processing of message.",
              x);
        }
      } finally {
        if (eventHandler.isPresent()) {
          eventHandler.get().deleteContext(connectionContext, inputProtocol, outputProtocol);
        }
        if (inputTransport != null) {
          inputTransport.close();
        }
        if (outputTransport != null) {
          outputTransport.close();
        }
        if (client_.isOpen()) {
          client_.close();
        }
      }
    }

    private boolean isIgnorableException(Exception x) {
      TTransportException tTransportException = null;

      if (x instanceof TTransportException) {
        tTransportException = (TTransportException) x;
      } else if (x.getCause() instanceof TTransportException) {
        tTransportException = (TTransportException) x.getCause();
      }

      if (tTransportException != null) {
        switch (tTransportException.getType()) {
          case TTransportException.END_OF_FILE:
          case TTransportException.TIMED_OUT:
            return true;
        }
      }
      return false;
    }
  }
}
