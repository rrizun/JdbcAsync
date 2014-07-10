import java.io.*;

public class GlobalDatabaseWriterTest {
//  static DataSources dataSources = new DataSources();
  static GlobalDatabaseWriter writer = new GlobalDatabaseWriter(dataSources.get());
  public static void main(String[] args) throws Exception {
    writer.startAsync().awaitRunning();
    try {
      for (int i = 0; i < 25; ++i)
        writer.writeAsync("asdf"+i);
      // ----------------------------------------------------------------------
      System.in.read();
      // ----------------------------------------------------------------------
    } finally {
      writer.stopAsync().awaitTerminated();
    }
    out.println("yeah!");
  }
  static PrintStream out = System.out;
}
