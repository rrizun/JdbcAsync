

import java.io.*;
import java.util.concurrent.*;

import javax.sql.*;

import com.google.common.collect.*;
import com.google.common.util.concurrent.*;

public class GlobalDatabaseWriter extends AbstractExecutionThreadService {
  
  static class WriteCtx {
    public final String sql;
    public final Object[] args;
    public WriteCtx(String sql, Object... args) {
      this.sql = sql;
      this.args = args;
    }
  }

  private final DataSource dataSource;
  private final BlockingDeque<WriteCtx> deque = Queues.newLinkedBlockingDeque();
  /**
   * ctor
   */
  public GlobalDatabaseWriter(DataSource dataSource) {
    this.dataSource = dataSource;
  }
  /**
   * writeAsync
   */
  public void writeAsync(String sql) {
    deque.addLast(new WriteCtx(sql));
  }
  @Override
  protected void startUp() throws Exception {
  }
  @Override
  protected void shutDown() throws Exception {
  }
  @Override
  protected void triggerShutdown() {
    deque.addLast(new WriteCtx(""));
  }
  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      WriteCtx writeCtx = deque.takeFirst();
      String sql = writeCtx.sql;
      Object[] args = writeCtx.args;
      if (!"".equals(writeCtx.sql)) { // sentinel value
        out.println(String.format("writeAsync sql=%s", sql));
        try {
          // db write
          new SqlHelper(dataSource).exec(sql, args);
        } catch (Exception e) {
          err.println(e.getMessage());
          deque.putFirst(writeCtx); // undo
          Thread.sleep(2000);
        }
      }
    }
  }
  static PrintStream out = System.out;
  static PrintStream err = System.err;
}
