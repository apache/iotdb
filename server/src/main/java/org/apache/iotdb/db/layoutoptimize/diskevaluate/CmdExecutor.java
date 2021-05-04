package org.apache.iotdb.db.layoutoptimize.diskevaluate;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class CmdExecutor {
  private static final Logger logger = Logger.getLogger(CmdExecutor.class.getSimpleName());
  private static final String SUDO_CMD = "sudo";
  private static final String SHELL_NAME = "/bin/bash";
  private static final String SHELL_PARAM = "-c";
  private static final String REDIRECT = "2>&1";
  private final String sudoPassword;
  private boolean verbose = true;
  private boolean errRedirect = true;
  // if it is synchronized
  private boolean sync = true;
  private String cmdSeparator = " && ";

  private List<String> cmds = new ArrayList<String>(16);

  public static CmdExecutor builder() {
    return new CmdExecutor();
  }

  public static CmdExecutor builder(String sudoPasword) {
    return new CmdExecutor(sudoPasword);
  }

  protected CmdExecutor() {
    this(null);
  }

  protected CmdExecutor(String sudoPasword) {
    this.sudoPassword = sudoPasword;
  }

  public CmdExecutor verbose(boolean verbose) {
    this.verbose = verbose;
    return this;
  }

  public CmdExecutor errRedirect(boolean errRedirect) {
    this.errRedirect = errRedirect;
    return this;
  }

  public CmdExecutor sync(boolean sync) {
    this.sync = sync;
    return this;
  }

  public CmdExecutor cmdSeparator(String cmdSeparator) {
    if (null != cmdSeparator && !cmdSeparator.isEmpty()) {
      this.cmdSeparator = cmdSeparator;
    }
    return this;
  }

  private String getRedirect() {
    return errRedirect ? REDIRECT : "";
  }

  /**
   * add a sudo command
   *
   * @param cmd the command need to be executed(should not contain "sudo")
   * @return
   */
  public CmdExecutor sudoCmd(String cmd) {
    if (null != cmd && 0 != cmd.length()) {
      if (null == sudoPassword) {
        cmds.add(String.format("%s %s %s", SUDO_CMD, cmd, getRedirect()));
      } else {
        cmds.add(String.format("echo '%s' | %s %s %s", sudoPassword, SUDO_CMD, cmd, getRedirect()));
      }
    }
    return this;
  }

  /**
   * add a simple command
   *
   * @param cmd the command need to be executed
   * @return
   */
  public CmdExecutor cmd(String cmd) {
    if (null != cmd && 0 != cmd.length()) {
      cmds.add(String.format("%s %s", cmd, getRedirect()));
    }
    return this;
  }

  private List<String> build() {
    return cmds.isEmpty()
        ? Collections.<String>emptyList()
        : Arrays.asList(SHELL_NAME, SHELL_PARAM, join(cmds, cmdSeparator));
  }

  private static String join(List<String> strs, String separator) {
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < strs.size(); ++i) {
      if (i > 0) {
        buffer.append(separator);
      }
      buffer.append(strs.get(i));
    }
    return buffer.toString();
  }

  /**
   * output the content in {@link InputStream} to {@link StringBuffer}
   *
   * @param in the input stream containing the content needs to be transfer
   * @return
   * @throws IOException
   */
  private static void toBuffer(InputStream in, StringBuffer buffer) throws IOException {
    if (null == in || null == buffer) {
      return;
    }
    InputStreamReader ir = new InputStreamReader(in);
    LineNumberReader input = new LineNumberReader(ir);
    try {
      String line;
      while ((line = input.readLine()) != null) {
        buffer.append(line).append("\n");
      }
    } finally {
      input.close();
    }
  }

  /**
   * execute the command using {@link Runtime#exec(String[])}
   *
   * @return the result of execution
   */
  public String exec() throws IOException {
    StringBuffer outBuffer = new StringBuffer();
    exec(outBuffer, null);
    return outBuffer.toString();
  }

  /**
   * execute the command using {@link Runtime#exec(String[])}
   *
   * @param outBuffer standard output buffer
   * @param errBuffer standard error buffer
   * @throws IOException
   */
  public void exec(StringBuffer outBuffer, StringBuffer errBuffer) throws IOException {
    List<String> cmdlist = build();
    if (!cmdlist.isEmpty()) {
      if (verbose) {
        logger.info(join(cmdlist, " "));
      }
      Process process = Runtime.getRuntime().exec(cmdlist.toArray(new String[cmdlist.size()]));
      if (sync) {
        try {
          process.waitFor();
        } catch (InterruptedException e) {
        }
      }
      toBuffer(process.getInputStream(), outBuffer);
      toBuffer(process.getErrorStream(), errBuffer);
    }
  }
}
