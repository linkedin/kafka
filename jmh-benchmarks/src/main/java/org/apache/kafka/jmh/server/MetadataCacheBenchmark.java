package org.apache.kafka.jmh.server;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import kafka.server.MetadataCache;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import scala.collection.JavaConverters;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 60, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 60, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
public class MetadataCacheBenchmark {

  @State(Scope.Thread)
  public static class BenchState {

    @Setup(Level.Iteration)
    public void setUp() {
      UpdateMetadataRequest request = new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion(),
          2, 1, 0, 0, getPartitionStates(),
          getUpdateMetadataBroker()).build();
      metadataCache.updateMetadata(15, request);
    }

    private List<UpdateMetadataRequestData.UpdateMetadataBroker> getUpdateMetadataBroker() {
      List<UpdateMetadataRequestData.UpdateMetadataBroker> result = new LinkedList<>();
      for (int i = 0; i < 5; i++) {
        result.add(new UpdateMetadataRequestData.UpdateMetadataBroker()
            .setId(i)
            .setEndpoints(getEndPoints(i))
            .setRack("rack1"));
      }
      return result;
    }

    private List<UpdateMetadataRequestData.UpdateMetadataEndpoint> getEndPoints(int brokerId) {
      return new LinkedList<UpdateMetadataRequestData.UpdateMetadataEndpoint>() {
        {
          add(new UpdateMetadataRequestData.UpdateMetadataEndpoint()
              .setHost("host-" + brokerId)
              .setPort(9092)
              .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
              .setListener(ListenerName.forSecurityProtocol(org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT).value())
          );
        }
        {
          add(new UpdateMetadataRequestData.UpdateMetadataEndpoint()
              .setHost("host-" + brokerId)
              .setPort(9093)
              .setSecurityProtocol(SecurityProtocol.SSL.id)
              .setListener(ListenerName.forSecurityProtocol(org.apache.kafka.common.security.auth.SecurityProtocol.SSL).value())
          );
        }
      };
    }

    private List<UpdateMetadataRequestData.UpdateMetadataPartitionState> getPartitionStates() {
      List<UpdateMetadataRequestData.UpdateMetadataPartitionState> result = new LinkedList<>();
      int controllerEpoch = 1;
      int zkVersion = 3;

      result.add(new UpdateMetadataRequestData.UpdateMetadataPartitionState()
        .setTopicName(topicName)
          .setPartitionIndex(0)
          .setControllerEpoch(controllerEpoch)
          .setLeader(0)
          .setLeaderEpoch(0)
          .setIsr(Arrays.asList(0, 1, 3))
          .setZkVersion(zkVersion)
          .setReplicas(Arrays.asList(0, 1, 3))
      );
      result.add(new UpdateMetadataRequestData.UpdateMetadataPartitionState()
          .setTopicName(topicName)
          .setPartitionIndex(1)
          .setControllerEpoch(controllerEpoch)
          .setLeader(1)
          .setLeaderEpoch(1)
          .setIsr(Arrays.asList(1, 0))
          .setZkVersion(zkVersion)
          .setReplicas(Arrays.asList(1, 2, 0, 4))
      );
      result.add(new UpdateMetadataRequestData.UpdateMetadataPartitionState()
          .setTopicName(topic1Name)
          .setPartitionIndex(0)
          .setControllerEpoch(controllerEpoch)
          .setLeader(2)
          .setLeaderEpoch(2)
          .setIsr(Arrays.asList(2, 1))
          .setZkVersion(zkVersion)
          .setReplicas(Arrays.asList(2, 1, 3))
      );
      return result;
    }

    public final int brokerId = 1;
    public final MetadataCache metadataCache = new MetadataCache(brokerId);
    public final String topicName = "topic";
    public final String topic1Name = "topic1";
    public final ListenerName listenerName = ListenerName.normalised("PLAINTEXT");
    public final scala.collection.Set<String> topicScalaSetInQuery = JavaConverters.<String>asScalaSet(Collections.singleton(topicName));
  }
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void benchmarkGetTopicMetadata(BenchState state, Blackhole blackhole) {
    state.metadataCache.getTopicMetadata(state.topicScalaSetInQuery, state.listenerName, false, false);
  }
}
