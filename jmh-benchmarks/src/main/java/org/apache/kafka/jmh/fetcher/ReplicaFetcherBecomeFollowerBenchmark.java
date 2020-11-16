/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.jmh.fetcher;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * This benchmark measures the time it takes to run the ReplicaManager#becomeLeaderOrFollower method,
 * which roughly corresponds the LeaderAndIsr request local processing time.
 * We are using a hypothetical case where a follower broker (broker 100) needs to switch 50% of its partitions' leaders
 * from broker 101 to broker 102 or vice versa.
 */
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ReplicaFetcherBecomeFollowerBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaFetcherBecomeFollowerBenchmark.class);

    @Param({"10000"})
    private int partitionsCount;

    @Param("100")
    private int brokerCount;

    @Param("2")
    private int numReplicaFetchers;

    private ReplicaFetcherTest replicaFetcherTest;
    @Setup(Level.Iteration)
    public void setup() {
        replicaFetcherTest = new ReplicaFetcherTest(partitionsCount, brokerCount, numReplicaFetchers);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        replicaFetcherTest.shutdown();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void testProcessFirstLeaderAndIsr() {
        replicaFetcherTest.becomeLeaderOrFollower();
    }
}
