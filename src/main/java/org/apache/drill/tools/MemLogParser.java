package org.apache.drill.tools;

import edu.princeton.cs.introcs.Draw;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class MemLogParser {
  // keep track of numChunks
  private static List<Long> data = null; // num chunks per different timestamp
  private static long max = -1; // max num chunks

  public static void main(String[] args) throws IOException {
    Options options = new Options();
    CmdLineParser parser = new CmdLineParser(options);

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }

     LineReader reader;

    data = new ArrayList<Long>();

    try {
      final BufferedReader bufferedReader = new BufferedReader(new FileReader(options.input));

      EventHandler handler = null;
      if (options.output == Options.Output.EVENT) {
        handler = new EventHandler() {
          public void event(final Timestamp timestamp, String evt) {
            handleEvent(timestamp, evt);
          }
        };
      }

      reader = new LineReader(bufferedReader, handler);
    } catch (FileNotFoundException e) {
      System.err.printf("File '%s' not found%n", options.input);
      return;
    }

    final NettyLogParser logparser = new NettyLogParser(reader, options);

    // start by printing first line of each minute
    switch (options.output) {
      case CHUNK:
        System.out.println("timestamp,arena,chunk,used-bytes");
        break;
      case EVENT:
        System.out.println("timestamp,chunk,req_capacity,norm_capacity,thread");
        break;
      default:
        System.err.println("Unknown output " + options.output);
        System.exit(-1);
    }

    try {
      while (logparser.parseArenas()) ;
    } finally {
      reader.close();
    }

    // draw data points
    if (options.draw) {
      drawGraph(options);
    }
  }

  private static void handleEvent(final Timestamp timestamp, final String event) {
    // <timestamp> <newChunk> <hashcode> req_capacity <req_capacity> norm_capacity <norm-capacity> [<thread>]
    final String[] parts = event.split(" ");
    final String hashCode = parts[1];
    final long reqCapacity = Long.parseLong(parts[3]);
    final long normCapacity = Long.parseLong(parts[5]);
    final String thread = parts[6].substring(1, parts[6].length() - 1);

    System.out.printf("\"%s\",%s,%d,%d,\"%s\"%n", timestamp, hashCode, reqCapacity, normCapacity, thread);
  }
  /**
   * draws a graph showing the evolution of allocated chunks over time
   * @param options
   */
  private static void drawGraph(Options options) {
    final Draw window = new Draw(options.input);
    window.setCanvasSize(1024, 512);
    int num = data.size();

    for (int i = 0; i < num; i++) {
      final double x = i * 1.0 / num;
      final double h = ((double) data.get(i)) / max;
      window.filledRectangle(x, h * .5, .25 / num, h * .5);
    }
  }

  private static class NettyLogParser {
    final LineReader reader;
    final Options options;

    final Timestamp start;
    boolean ignore = false;

    Timestamp current;

    NettyLogParser(final LineReader reader, final Options options) {
      this.reader = reader;
      this.options = options;

      if (options.start != null && !options.start.isEmpty()) {
        ignore = true;
        start = Timestamp.valueOf(options.start);
      } else {
        start = null;
      }
    }

    boolean parseArenas() throws IOException {
      String line;
      boolean firstLine = true;

      while ((line = reader.nextLine()) != null) {
        if (line.isEmpty()) {
          return !firstLine;
        }

        if (firstLine) {
          // extract timestamp
          String[] parts = line.split(" ");
          String tm = parts[0] + " " + parts[1].split(",")[0];
          try {
            current = Timestamp.valueOf(tm);
          } catch (IllegalArgumentException e) {
            System.err.printf("error parsing '%s'%n", tm);
            throw e;
          }
          if (ignore && !current.before(start)) {
            ignore = false;
          }

          firstLine = false;
        } else {
          int numArenas = Integer.parseInt(line.split(" ")[0]);
          long numChunks = 0;
          for (int i = 0; i < numArenas; i++) {
            numChunks += parseArena(i);
          }
          reader.nextLine(); // skip large buffers outstanding
          reader.nextLine(); // skip normal buffers outstanding
          reader.nextLine(); // skip empty line

          if (!ignore) {
            data.add(numChunks);
            if (max < numChunks) {
              max = numChunks;
            }
          }
          break;
        }
      }

      return !firstLine;
    }

    /**
     * @param arena current arena
     * @return num allocated chunks in this arena, 0 if ignore = true
     * @throws IOException
     */
    long parseArena(final int arena) throws IOException {
      String line;
      long numChunks = 0;

      // start reading chunks
      // we have 6 categories: 0~25%, 0~50%, 25~75%, 50~100%, 75~100%, 100%
      for (int cat = 0; cat < 6; cat++) {
        reader.nextLine(); // read (and ignore) 1st category header
        line = reader.nextLine(); // read category content
        if ("none".equals(line)) {
          // empty category, read next line. could be category header or next line
        } else {
          while (line.startsWith("Chunk(") && !line.startsWith("Chunk(s)")) {
            // this is an allocated chunk
            if (!ignore) {
              if (options.output == Options.Output.CHUNK) {
                parseChunk(arena, line);
              }
              numChunks++;
            }
            line = reader.nextLine();
          }
          reader.revertLine();
        }
      }

      // skipping tiny and small subpages
      while((line = reader.nextLine()) != null && !line.isEmpty()) {
        if (line.startsWith("Chunk(s)") || line.startsWith("Large")) {
          reader.revertLine();
          break;
        }
      }

      return numChunks;
    }

    void parseChunk(final int arena, final String chunkLine) {
      // Chunk(<hashcode>: <usage>%, <used-bytes>/<chunk-size>)
      String[] parts = chunkLine.split(" ");
      final String hashCode = parts[0].substring(6, parts[0].length() - 1);
      String[] subParts = parts[2].split("/");
      long usedBytes = Long.parseLong(subParts[0]);

      System.out.printf("\"%s\",%d,%s,%d%n", current, arena, hashCode, usedBytes);
    }

  }



  private static class LineReader {
    private String line;
    private boolean readNext = true;
    private final BufferedReader reader;
    private final EventHandler eventHandler;

    public LineReader(final BufferedReader reader) throws IOException {
      this(reader, null);
    }

    public LineReader(final BufferedReader reader, final EventHandler eventHandler) throws IOException {
      this.reader = reader;
      this.eventHandler = eventHandler;
    }

    public String nextLine() throws IOException {
      if (readNext) {
        line = readLine();
      }
      readNext = true;
      return line;
    }

    String readLine() throws IOException {
      String line = reader.readLine();
      if (line != null && line.contains("MEM_EVT")) {
        if (eventHandler != null) {
          processEvent(line);
        }
        reader.readLine(); // read empty line
        return readLine();
      }

      return line;
    }

    void processEvent(String line) {
      int delimiterIdx = line.indexOf("MEM_EVT");
      String[] parts = line.substring(0, delimiterIdx - 1).split(" ");
      String timestamp = parts[0] + " " + parts[1].split(",")[0];
      String event = line.substring(delimiterIdx + 8);
      eventHandler.event(Timestamp.valueOf(timestamp), event);
    }

    public void revertLine() {
      assert readNext : "we already reverted a line";
      readNext = false;
    }

    public void close() throws IOException {
      reader.close();
    }
  }

  private interface EventHandler {
    void event(final Timestamp timestamp, String line);
  }

  private static class Options {
    @Option(name = "-input", required = true)
    String input;
    @Option(name = "-start")
    String start = null;
    @Option(name = "-draw")
    boolean draw;
    @Option(name = "-output")
    Output output = Output.CHUNK;

    private enum Output {
      CHUNK, EVENT
    }
  }

}
