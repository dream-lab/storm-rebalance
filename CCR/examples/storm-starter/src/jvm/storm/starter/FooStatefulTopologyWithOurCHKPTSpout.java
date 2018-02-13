/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.spout.fooRandomIntegerWithCHKPTSpout;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example topology that demonstrates the use of {@link org.apache.storm.topology.IStatefulBolt}
 * to manage state. To run the example,
 * <pre>
 * $ storm jar examples/storm-starter/storm-starter-topologies-*.jar storm.starter.StatefulTopology statetopology
 * </pre>
 * <p/>
 * The default state used is 'InMemoryKeyValueState' which does not persist the state across restarts. You could use
 * 'RedisKeyValueState' to test state persistence by setting below property in conf/storm.yaml
 * <pre>
 * topology.state.provider: org.apache.storm.redis.state.RedisKeyValueStateProvider
 * </pre>
 * <p/>
 * You should also start a local redis instance before running the 'storm jar' command. The default
 * RedisKeyValueStateProvider parameters can be overridden in conf/storm.yaml, for e.g.
 * <p/>
 * <pre>
 * topology.state.provider.config: '{"keyClass":"...", "valueClass":"...",
 *                                   "keySerializerClass":"...", "valueSerializerClass":"...",
 *                                   "jedisPoolConfig":{"host":"localhost", "port":6379,
 *                                      "timeout":2000, "database":0, "password":"xyz"}}'
 *
 * </pre>
 * </p>
 */
public class FooStatefulTopologyWithOurCHKPTSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FooStatefulTopologyWithOurCHKPTSpout.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("spout", new OurRandomIntegerWithCHKPTSpout());

        builder.setSpout("spout", new fooRandomIntegerWithCHKPTSpout());
        builder.setBolt("fooPartial2", new foo("fooPartial2"), 1).shuffleGrouping("spout","datastream");
//        builder.setBolt("fooPartial2", new foo("fooPartial2"), 1).shuffleGrouping("spout").setNumTasks(1);
//        builder.setBolt("printer", new PrinterBolt(), 2).shuffleGrouping("partialsum2");
//        builder.setBolt("total", new StatefulSumBolt("total"), 1).shuffleGrouping("printer");
        Config conf = new Config();
//        conf.setNumWorkers(2);
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL, 10);
//        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,30); // in sec.
        conf.put(Config.TOPOLOGY_STATE_PROVIDER,"org.apache.storm.redis.state.RedisKeyValueStateProvider");
//        conf.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG,"{\"valueSerializerClass\":\"org.apache.storm.state.OurStateSerializerCopyDefault\"}");
//        conf.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG,"{\"valueSerializerClass\":\"org.apache.storm.state.OurStateSerializer\"}");
//        conf.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG,"{" +
//                "\"valueClass\":\"org.apache.storm.state.OurCustomPair1\","+
//                "\"valueSerializerClass\":\"org.apache.storm.state.OurStateSerializer\"}");


        System.out.println("TEST: inside main");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();
            cluster.submitTopology("test", conf, topology);
            Utils.sleep(400000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    /**
     * A bolt that uses {@link KeyValueState} to save its state.
     */

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple);
//            LOG.debug("Got tuple {}", tuple);
            System.out.println("Got tuple {}" + tuple);
            collector.emit(tuple.getValues());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            ofd.declare(new Fields("value"));
        }

    }
}
