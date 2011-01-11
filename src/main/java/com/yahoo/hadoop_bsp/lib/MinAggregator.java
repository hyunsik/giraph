package com.yahoo.hadoop_bsp.lib;

import org.apache.hadoop.io.DoubleWritable;

import com.yahoo.hadoop_bsp.Aggregator;

/**
 * Aggregator for getting min value.
 *
 **/

public class MinAggregator implements Aggregator<DoubleWritable> {

  private double min = Double.MAX_VALUE;

  public void aggregate(DoubleWritable value) {
      double val = value.get();
      if (val < min) {
          min = val;
      }   
  }

  public void setAggregatedValue(DoubleWritable value) {
      min = value.get();
  }

  public DoubleWritable getAggregatedValue() {
      return new DoubleWritable(min);
  }

  public DoubleWritable createAggregatedValue() {
      return new DoubleWritable();
  }
  
}
