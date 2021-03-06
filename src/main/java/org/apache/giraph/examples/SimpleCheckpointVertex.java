/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * An example that simply uses its id, value, and edges to compute new data
 * every iteration to verify that checkpoint restarting works.  Fault injection
 * can also test automated checkpoint restarts.
 */
public class SimpleCheckpointVertex extends
    EdgeListVertex<LongWritable, IntWritable, FloatWritable, FloatWritable>
    implements Tool {
  /** Which superstep to cause the worker to fail */
  public static final int FAULTING_SUPERSTEP = 4;
  /** Vertex id to fault on */
  public static final long FAULTING_VERTEX_ID = 1;
  /** Dynamically set number of supersteps */
  public static final String SUPERSTEP_COUNT =
      "simpleCheckpointVertex.superstepCount";
  /** Should fault? */
  public static final String ENABLE_FAULT =
      "simpleCheckpointVertex.enableFault";
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SimpleCheckpointVertex.class);
  /** Configuration */
  private Configuration conf;

  @Override
  public void compute(Iterable<FloatWritable> messages) {
    SimpleCheckpointVertexWorkerContext workerContext =
        (SimpleCheckpointVertexWorkerContext) getWorkerContext();

    LongSumAggregator sumAggregator = (LongSumAggregator)
        getAggregator(LongSumAggregator.class.getName());

    boolean enableFault = workerContext.getEnableFault();
    int supersteps = workerContext.getSupersteps();

    if (enableFault && (getSuperstep() == FAULTING_SUPERSTEP) &&
        (getContext().getTaskAttemptID().getId() == 0) &&
        (getId().get() == FAULTING_VERTEX_ID)) {
      LOG.info("compute: Forced a fault on the first " +
          "attempt of superstep " +
          FAULTING_SUPERSTEP + " and vertex id " +
          FAULTING_VERTEX_ID);
      System.exit(-1);
    }
    if (getSuperstep() > supersteps) {
      voteToHalt();
      return;
    }
    LOG.info("compute: " + sumAggregator);
    sumAggregator.aggregate(getId().get());
    LOG.info("compute: sum = " +
        sumAggregator.getAggregatedValue().get() +
        " for vertex " + getId());
    float msgValue = 0.0f;
    for (FloatWritable message : messages) {
      float curMsgValue = message.get();
      msgValue += curMsgValue;
      LOG.info("compute: got msgValue = " + curMsgValue +
          " for vertex " + getId() +
          " on superstep " + getSuperstep());
    }
    int vertexValue = getValue().get();
    setValue(new IntWritable(vertexValue + (int) msgValue));
    LOG.info("compute: vertex " + getId() +
        " has value " + getValue() +
        " on superstep " + getSuperstep());
    for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
      FloatWritable newEdgeValue = new FloatWritable(edge.getValue().get() +
          (float) vertexValue);
      LOG.info("compute: vertex " + getId() +
          " sending edgeValue " + edge.getValue() +
          " vertexValue " + vertexValue +
          " total " + newEdgeValue +
              " to vertex " + edge.getTargetVertexId() +
              " on superstep " + getSuperstep());
      addEdge(edge.getTargetVertexId(), newEdgeValue);
      sendMessage(edge.getTargetVertexId(), newEdgeValue);
    }
  }

  /**
   * Worker context associated with {@link SimpleCheckpointVertex}.
   */
  public static class SimpleCheckpointVertexWorkerContext
      extends WorkerContext {
    /** Filename to indicate whether a fault was found */
    public static final String FAULT_FILE = "/tmp/faultFile";
    /** User can access this after the application finishes if local */
    private static long FINAL_SUM;
    /** Number of supersteps to run (6 by default) */
    private int supersteps = 6;
    /** Enable the fault at the particular vertex id and superstep? */
    private boolean enableFault = false;

    public static long getFinalSum() {
      return FINAL_SUM;
    }

    @Override
    public void preApplication()
      throws InstantiationException, IllegalAccessException {
      registerAggregator(LongSumAggregator.class.getName(),
          LongSumAggregator.class);
      LongSumAggregator sumAggregator = (LongSumAggregator)
          getAggregator(LongSumAggregator.class.getName());
      sumAggregator.setAggregatedValue(0);
      supersteps = getContext().getConfiguration()
          .getInt(SUPERSTEP_COUNT, supersteps);
      enableFault = getContext().getConfiguration()
          .getBoolean(ENABLE_FAULT, false);
    }

    @Override
    public void postApplication() {
      LongSumAggregator sumAggregator = (LongSumAggregator)
          getAggregator(LongSumAggregator.class.getName());
      FINAL_SUM = sumAggregator.getAggregatedValue().get();
      LOG.info("FINAL_SUM=" + FINAL_SUM);
    }

    @Override
    public void preSuperstep() {
      useAggregator(LongSumAggregator.class.getName());
    }

    @Override
    public void postSuperstep() { }

    public int getSupersteps() {
      return this.supersteps;
    }

    public boolean getEnableFault() {
      return this.enableFault;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    options.addOption("v", "verbose", false, "Verbose");
    options.addOption("w",
        "workers",
        true,
        "Number of workers");
    options.addOption("s",
        "supersteps",
        true,
        "Supersteps to execute before finishing");
    options.addOption("w",
        "workers",
        true,
        "Minimum number of workers");
    options.addOption("o",
        "outputDirectory",
        true,
        "Output directory");
    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    if (cmd.hasOption('h')) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }
    if (!cmd.hasOption('w')) {
      LOG.info("Need to choose the number of workers (-w)");
      return -1;
    }
    if (!cmd.hasOption('o')) {
      LOG.info("Need to set the output directory (-o)");
      return -1;
    }

    GiraphJob bspJob = new GiraphJob(getConf(), getClass().getName());
    bspJob.setVertexClass(getClass());
    bspJob.setVertexInputFormatClass(GeneratedVertexInputFormat.class);
    bspJob.setVertexOutputFormatClass(SimpleTextVertexOutputFormat.class);
    bspJob.setWorkerContextClass(SimpleCheckpointVertexWorkerContext.class);
    int minWorkers = Integer.parseInt(cmd.getOptionValue('w'));
    int maxWorkers = Integer.parseInt(cmd.getOptionValue('w'));
    bspJob.setWorkerConfiguration(minWorkers, maxWorkers, 100.0f);

    FileOutputFormat.setOutputPath(bspJob.getInternalJob(),
                                   new Path(cmd.getOptionValue('o')));
    boolean verbose = false;
    if (cmd.hasOption('v')) {
      verbose = true;
    }
    if (cmd.hasOption('s')) {
      getConf().setInt(SUPERSTEP_COUNT,
          Integer.parseInt(cmd.getOptionValue('s')));
    }
    if (bspJob.run(verbose)) {
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Executable from the command line.
   *
   * @param args Command line args.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new SimpleCheckpointVertex(), args));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
