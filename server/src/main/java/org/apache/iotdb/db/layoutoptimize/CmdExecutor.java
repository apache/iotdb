package org.apache.iotdb.db.layoutoptimize;

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
  /** 执行 sudo 的密码 */
  private final String sudoPassword;
  /** 是否显示命令内容及输出 */
  private boolean verbose = true;
  /** 是否错误输出重定向 */
  private boolean errRedirect = true;
  /** 是否同步，主线程是否等待命令执行结束 */
  private boolean sync = true;
  /** 执行多条命令时的命令分隔符 */
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
   * 添加一条需要sudo执行的命令
   *
   * @param cmd 要执行的命令(字符串中不需要有sudo)
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
   * 添加一条普通命令
   *
   * @param cmd
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
   * 将{@link InputStream}中所有内容输出到{@link StringBuffer}
   *
   * @param in
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
   * 调用{@link Runtime#exec(String[])}执行命令
   *
   * @return 返回输出结果
   */
  public String exec() throws IOException {
    StringBuffer outBuffer = new StringBuffer();
    exec(outBuffer, null);
    return outBuffer.toString();
  }

  /**
   * 调用{@link Runtime#exec(String[])}执行命令
   *
   * @param outBuffer 标准输出
   * @param errBuffer 错误信息输出
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
