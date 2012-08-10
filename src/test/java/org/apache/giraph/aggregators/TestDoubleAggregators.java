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
package org.apache.giraph.aggregators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.io.DoubleWritable;
import org.junit.Test;

public class TestDoubleAggregators {

  @Test
  public void testMaxAggregator() {
    DoubleMaxAggregator max = new DoubleMaxAggregator();
    max.aggregate(new DoubleWritable(2.0));
    max.aggregate(new DoubleWritable(3.0));
    assertEquals(3.0, max.getAggregatedValue().get());
    max.setAggregatedValue(new DoubleWritable(1.0));
    assertEquals(1.0, max.getAggregatedValue().get());
    DoubleWritable dw = max.createInitialValue();
    assertNotNull(dw);
  }

  @Test
  public void testMinAggregator() {
    DoubleMinAggregator min = new DoubleMinAggregator();
    min.aggregate(new DoubleWritable(3.0));
    min.aggregate(new DoubleWritable(2.0));
    assertEquals(2.0, min.getAggregatedValue().get());
    min.setAggregatedValue(new DoubleWritable(3.0));
    assertEquals(3.0, min.getAggregatedValue().get());
    DoubleWritable dw = min.createInitialValue();
    assertNotNull(dw);
  }

  @Test
  public void testOverwriteAggregator() {
    DoubleOverwriteAggregator overwrite = new DoubleOverwriteAggregator();
    overwrite.aggregate(new DoubleWritable(1.0));
    assertEquals(1.0, overwrite.getAggregatedValue().get());
    overwrite.aggregate(new DoubleWritable(2.0));
    assertEquals(2.0, overwrite.getAggregatedValue().get());
    overwrite.setAggregatedValue(new DoubleWritable(3.0));
    assertEquals(3.0, overwrite.getAggregatedValue().get());
    DoubleWritable dw = overwrite.createInitialValue();
    assertNotNull(dw);
  }

  @Test
  public void testProductAggregator() {
    DoubleProductAggregator product = new DoubleProductAggregator();
    product.aggregate(new DoubleWritable(6.0));
    product.aggregate(new DoubleWritable(7.0));
    assertEquals(42.0, product.getAggregatedValue().get());
    product.setAggregatedValue(new DoubleWritable(1.0));
    assertEquals(1.0, product.getAggregatedValue().get());
    DoubleWritable dw = product.createInitialValue();
    assertNotNull(dw);
  }

  @Test
  public void testSumAggregator() {
    DoubleSumAggregator sum = new DoubleSumAggregator();
    sum.aggregate(new DoubleWritable(1.0));
    sum.aggregate(new DoubleWritable(2.0));
    assertEquals(3.0, sum.getAggregatedValue().get());
    sum.setAggregatedValue(new DoubleWritable(4.0));
    assertEquals(4.0, sum.getAggregatedValue().get());
    DoubleWritable dw = sum.createInitialValue();
    assertNotNull(dw);
  }

}
