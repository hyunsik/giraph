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

package org.apache.giraph.comm;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This server uses Netty and will implement all Giraph communication
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class NettyServer<I extends WritableComparable,
     V extends Writable, E extends Writable,
     M extends Writable> {
  /** Default maximum thread pool size */
  public static final int DEFAULT_MAXIMUM_THREAD_POOL_SIZE = 64;
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyServer.class);
  /** Configuration */
  private final Configuration conf;
  /** Factory of channels */
  private ChannelFactory channelFactory;
  /** Accepted channels */
  private final ChannelGroup accepted = new DefaultChannelGroup();
  /** Worker thread pool (if implemented as a ThreadPoolExecutor) */
  private ThreadPoolExecutor workerThreadPool = null;
  /** Local hostname */
  private final String localHostname;
  /** Address of the server */
  private InetSocketAddress myAddress;
  /** Maximum number of threads */
  private final int maximumPoolSize;
  /** Request reqistry */
  private final RequestRegistry requestRegistry = new RequestRegistry();
  /** Server data */
  private final ServerData<I, V, E, M> serverData;
  /** Server bootstrap */
  private ServerBootstrap bootstrap;

  /**
   * Constructor for creating the server
   *
   * @param conf Configuration to use
   * @param serverData Server data to operate on
   */
  public NettyServer(Configuration conf, ServerData<I, V, E, M> serverData) {
    this.conf = conf;
    this.serverData = serverData;
    requestRegistry.registerClass(
        new SendVertexRequest<I, V, E, M>());
    requestRegistry.registerClass(
        new SendPartitionMessagesRequest<I, V, E, M>());
    requestRegistry.registerClass(
        new SendPartitionMutationsRequest<I, V, E, M>());
    requestRegistry.shutdown();

    ThreadFactory bossFactory = new ThreadFactoryBuilder()
      .setNameFormat("Giraph Netty Boss #%d")
      .build();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
      .setNameFormat("Giraph Netty Worker #%d")
      .build();
    try {
      this.localHostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("NettyServer: unable to get hostname");
    }
    maximumPoolSize = conf.getInt(GiraphJob.MSG_NUM_FLUSH_THREADS,
                                  DEFAULT_MAXIMUM_THREAD_POOL_SIZE);
    try {
      workerThreadPool =
        (ThreadPoolExecutor) Executors.newCachedThreadPool(workerFactory);
      workerThreadPool.setMaximumPoolSize(maximumPoolSize);
    } catch (ClassCastException e) {
      LOG.warn("Netty worker thread pool is not of type ThreadPoolExecutor", e);
    }
    channelFactory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(bossFactory),
        workerThreadPool);
  }

  /**
   * Start the server with the appropriate port
   */
  public void start() {
    bootstrap = new ServerBootstrap(channelFactory);
    // Set up the pipeline factory.
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
            new LengthFieldBasedFrameDecoder(1024 * 1024 * 1024, 0, 4, 0, 4),
            new RequestDecoder<I, V, E, M>(conf, requestRegistry),
            new RequestServerHandler<I, V, E, M>(serverData));
      }
    });

    int taskId = conf.getInt("mapred.task.partition", -1);
    int numTasks = conf.getInt("mapred.map.tasks", 1);
    int numWorkers = conf.getInt(GiraphJob.MAX_WORKERS, numTasks);
    int portIncrementConstant =
        (int) Math.pow(10, Math.ceil(Math.log10(numWorkers)));
    int bindPort = conf.getInt(GiraphJob.RPC_INITIAL_PORT,
        GiraphJob.RPC_INITIAL_PORT_DEFAULT) +
        taskId;
    int bindAttempts = 0;
    final int maxRpcPortBindAttempts =
        conf.getInt(GiraphJob.MAX_RPC_PORT_BIND_ATTEMPTS,
            GiraphJob.MAX_RPC_PORT_BIND_ATTEMPTS_DEFAULT);
    final boolean failFirstPortBindingAttempt =
        conf.getBoolean(GiraphJob.FAIL_FIRST_RPC_PORT_BIND_ATTEMPT,
            GiraphJob.FAIL_FIRST_RPC_PORT_BIND_ATTEMPT_DEFAULT);

    // Simple handling of port collisions on the same machine while
    // preserving debugability from the port number alone.
    // Round up the max number of workers to the next power of 10 and use
    // it as a constant to increase the port number with.
    boolean tcpNoDelay = false;
    boolean keepAlive = false;
    while (bindAttempts < maxRpcPortBindAttempts) {
      this.myAddress = new InetSocketAddress(localHostname, bindPort);
      if (failFirstPortBindingAttempt && bindAttempts == 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info("NettyServer: Intentionally fail first " +
              "binding attempt as giraph.failFirstRpcPortBindAttempt " +
              "is true, port " + bindPort);
        }
        ++bindAttempts;
        bindPort += portIncrementConstant;
        continue;
      }

      try {
        Channel ch = bootstrap.bind(myAddress);
        accepted.add(ch);
        tcpNoDelay = ch.getConfig().setOption("tcpNoDelay", true);
        keepAlive = ch.getConfig().setOption("keepAlive", true);
        break;
      } catch (ChannelException e) {
        LOG.warn("start: Likely failed to bind on attempt " +
            bindAttempts + " to port " + bindPort, e);
        ++bindAttempts;
        bindPort += portIncrementConstant;
      }
    }
    if (bindAttempts == maxRpcPortBindAttempts || myAddress == null) {
      throw new IllegalStateException(
          "start: Failed to start NettyServer with " +
              bindAttempts + " attempts");
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("start: Started server " +
          "communication server: " + myAddress + " with up to " +
          maximumPoolSize + " threads on bind attempt " + bindAttempts +
          " with tcpNoDelay = " + tcpNoDelay + " and keepAlive = " +
          keepAlive);
    }
  }

  /**
   * Stop the server.
   */
  public void stop() {
    accepted.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    bootstrap.releaseExternalResources();
  }

  public InetSocketAddress getMyAddress() {
    return myAddress;
  }
}

