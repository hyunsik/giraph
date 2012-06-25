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

package org.apache.giraph.benchmark;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * It stores a graph data set from a given input format to HDFS.
 */
public class GraphGenerator extends EdgeListVertex<LongWritable, DoubleWritable,
    DoubleWritable, DoubleWritable>
    implements Tool {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GraphGenerator.class);
  /** Configuration from Configurable */
  private Configuration conf;

  private final String [][] requiredOptions =
    {{"w", "Need to choose the number of workers (-w)"},
    {"gd", "Need to set graph distribution (-gd)"},
    {"of", "Need to set outputformat (-of)"},
    {"op", "Need to set output path (-op)"},
    {"V", "Need to set the number of vertices (-V)"},
    {"e", "Need to set the number of edges per vertex (-e)"}};

  @Override
  public void compute(Iterator<DoubleWritable> msgIterator) {
    voteToHalt();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    options.addOption("q", "quiet", false, "Quiet output");
    options.addOption("w",
        "workers",
        true,
        "Number of workers");
    options.addOption("gd",
        "distribution",
        true,
        "Graph distribution - (default: random)");
    options.addOption("of",
        "outputFormat",
        true,
        "Graph outputformat");
    options.addOption("op",
        "outputPath",
        true,
        "Graph output path");
    options.addOption("V",
        "vertex",
        true,
        "Number of vertex");
    options.addOption("e",
        "edgesPerVertex",
        true,
        "Edges per vertex");
    HelpFormatter formatter = new HelpFormatter();

    if (args.length == 0) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    // Verify all the options have been provided
    for (String[] requiredOption : requiredOptions) {
      if (!cmd.hasOption(requiredOption[0])) {
        LOG.info(requiredOption[1]);
        return -1;
      }
    }

    int workers = Integer.parseInt(cmd.getOptionValue('w'));
    GiraphJob job = new GiraphJob(getConf(), "GraphGenerator");
    job.setVertexClass(GraphGenerator.class);

    String graphDistribution = cmd.getOptionValue("gd");
    if (graphDistribution.equals("random")) {
      job.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
    } else {
      LOG.info(graphDistribution + "is not supported yet.");
      System.exit(-1);
    }

    job.setVertexOutputFormatClass(Class.forName(cmd.getOptionValue("of")));
    FileOutputFormat.setOutputPath(job.getInternalJob(),
        new Path(cmd.getOptionValue("op")));
    // If another VertexInputFormat is supported, the below constants
    // should be unified for all VertexInputFormat.
    job.getConfiguration().setLong(
        PseudoRandomVertexInputFormat.AGGREGATE_VERTICES,
        Long.parseLong(cmd.getOptionValue('V')));
    job.getConfiguration().setLong(
        PseudoRandomVertexInputFormat.EDGES_PER_VERTEX,
        Long.parseLong(cmd.getOptionValue('e')));
    job.setWorkerConfiguration(workers, workers, 100.0f);

    if (job.run(true)) {
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Execute the benchmark.
   *
   * @param args Typically the command line arguments.
   * @throws Exception Any exception from the computation.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new GraphGenerator(), args));
  }
}
