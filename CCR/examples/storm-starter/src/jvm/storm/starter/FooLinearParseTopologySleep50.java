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
import org.apache.storm.starter.genevents.factory.ArgumentClass;
import org.apache.storm.starter.genevents.factory.ArgumentParser;
import org.apache.storm.starter.spout.SampleSpoutWithCHKPTSpout;
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
public class FooLinearParseTopologySleep50 {
    private static final Logger LOG = LoggerFactory.getLogger(FooLinearParseTopologySleep50.class);


    private static final String[] PREPARE_STREAM_ID_list = {"PREPARE_STREAM_ID1", "PREPARE_STREAM_ID2", "PREPARE_STREAM_ID3", "PREPARE_STREAM_ID4",
            "PREPARE_STREAM_ID5", "PREPARE_STREAM_ID6", "PREPARE_STREAM_ID7", "PREPARE_STREAM_ID9",
            "PREPARE_STREAM_ID10", "PREPARE_STREAM_ID11", "PREPARE_STREAM_ID12", "PREPARE_STREAM_ID13",
            "PREPARE_STREAM_ID14", "PREPARE_STREAM_ID15", "PREPARE_STREAM_ID16", "PREPARE_STREAM_ID17",
            "PREPARE_STREAM_ID18", "PREPARE_STREAM_ID19", "PREPARE_STREAM_ID20", "PREPARE_STREAM_ID21",
            "PREPARE_STREAM_ID22","PREPARE_STREAM_ID23","PREPARE_STREAM_ID24","PREPARE_STREAM_ID25",
            "PREPARE_STREAM_ID26","PREPARE_STREAM_ID27","PREPARE_STREAM_ID28","PREPARE_STREAM_ID29",
            "PREPARE_STREAM_ID30","PREPARE_STREAM_ID31","PREPARE_STREAM_ID32","PREPARE_STREAM_ID33",
            "PREPARE_STREAM_ID34","PREPARE_STREAM_ID35","PREPARE_STREAM_ID36","PREPARE_STREAM_ID37",
            "PREPARE_STREAM_ID38","PREPARE_STREAM_ID39","PREPARE_STREAM_ID40","PREPARE_STREAM_ID41",
            "PREPARE_STREAM_ID42","PREPARE_STREAM_ID43","PREPARE_STREAM_ID44","PREPARE_STREAM_ID45",
            "PREPARE_STREAM_ID46","PREPARE_STREAM_ID47","PREPARE_STREAM_ID48","PREPARE_STREAM_ID49",
            "PREPARE_STREAM_ID50","PREPARE_STREAM_ID51","PREPARE_STREAM_ID52","PREPARE_STREAM_ID53"};

    public static void main(String[] args) throws Exception {

        /** Common Code begins **/
        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }


        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;

        String taskPropFilename=argumentClass.getTasksPropertiesFilename();
        System.out.println("taskPropFilename-"+taskPropFilename);

        TopologyBuilder builder = new TopologyBuilder();

        //        builder.setSpout("spout", new OurRandomIntegerWithCHKPTSpout());
//        builder.setSpout("spout", new fooRandomIntegerWithCHKPTSpout());
        builder.setSpout("spout", new SampleSpoutWithCHKPTSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()), 1);

        // with direct stream
        builder.setBolt("fooPartial2", new fooSleep("2"), 1).setNumTasks(1).shuffleGrouping("spout")
                .directGrouping("spout", PREPARE_STREAM_ID_list[0]);
        builder.setBolt("fooPartial3", new fooSleep("3"), 1).setNumTasks(1).shuffleGrouping("fooPartial2")
                .directGrouping("spout", PREPARE_STREAM_ID_list[1]);
        builder.setBolt("fooPartial4", new fooSleep("4"), 1).setNumTasks(1).shuffleGrouping("fooPartial3")
                .directGrouping("spout", PREPARE_STREAM_ID_list[2]);
        builder.setBolt("fooPartial5", new fooSleep("5"), 1).shuffleGrouping("fooPartial4")
                .directGrouping("spout", PREPARE_STREAM_ID_list[3]);
        builder.setBolt("fooPartial6", new fooSleep("6"), 1).shuffleGrouping("fooPartial5")
                .directGrouping("spout", PREPARE_STREAM_ID_list[4]);


        builder.setBolt("fooPartial7", new fooSleep("7"), 1).shuffleGrouping("fooPartial6").directGrouping("spout", PREPARE_STREAM_ID_list[5]);
        builder.setBolt("fooPartial8", new fooSleep("8"), 1).shuffleGrouping("fooPartial7").directGrouping("spout", PREPARE_STREAM_ID_list[6]);
        builder.setBolt("fooPartial9", new fooSleep("9"), 1).shuffleGrouping("fooPartial8").directGrouping("spout", PREPARE_STREAM_ID_list[7]);
        builder.setBolt("fooPartial10", new fooSleep("10"), 1).shuffleGrouping("fooPartial9").directGrouping("spout", PREPARE_STREAM_ID_list[8]);
        builder.setBolt("fooPartial11", new fooSleep("11"), 1).shuffleGrouping("fooPartial10").directGrouping("spout", PREPARE_STREAM_ID_list[9]);

        builder.setBolt("fooPartial12", new fooSleep("12"), 1).shuffleGrouping("fooPartial11").directGrouping("spout", PREPARE_STREAM_ID_list[10]);
        builder.setBolt("fooPartial13", new fooSleep("13"), 1).shuffleGrouping("fooPartial12").directGrouping("spout", PREPARE_STREAM_ID_list[11]);
        builder.setBolt("fooPartial14", new fooSleep("14"), 1).shuffleGrouping("fooPartial13").directGrouping("spout", PREPARE_STREAM_ID_list[12]);
        builder.setBolt("fooPartial15", new fooSleep("15"), 1).shuffleGrouping("fooPartial14").directGrouping("spout", PREPARE_STREAM_ID_list[13]);
        builder.setBolt("fooPartial16", new fooSleep("16"), 1).shuffleGrouping("fooPartial15").directGrouping("spout", PREPARE_STREAM_ID_list[14]);
        builder.setBolt("fooPartial17", new fooSleep("17"), 1).shuffleGrouping("fooPartial16").directGrouping("spout", PREPARE_STREAM_ID_list[15]);
        builder.setBolt("fooPartial18", new fooSleep("18"), 1).shuffleGrouping("fooPartial17").directGrouping("spout", PREPARE_STREAM_ID_list[16]);
        builder.setBolt("fooPartial19", new fooSleep("19"), 1).shuffleGrouping("fooPartial18").directGrouping("spout", PREPARE_STREAM_ID_list[17]);
        builder.setBolt("fooPartial20", new fooSleep("20"), 1).shuffleGrouping("fooPartial19").directGrouping("spout", PREPARE_STREAM_ID_list[18]);
        builder.setBolt("fooPartial21", new fooSleep("21"), 1).shuffleGrouping("fooPartial20").directGrouping("spout", PREPARE_STREAM_ID_list[19]);

        builder.setBolt("fooPartial22", new fooSleep("22"), 1).shuffleGrouping("fooPartial21").directGrouping("spout", PREPARE_STREAM_ID_list[20]);
        builder.setBolt("fooPartial23", new fooSleep("23"), 1).shuffleGrouping("fooPartial22").directGrouping("spout", PREPARE_STREAM_ID_list[21]);
        builder.setBolt("fooPartial24", new fooSleep("24"), 1).shuffleGrouping("fooPartial23").directGrouping("spout", PREPARE_STREAM_ID_list[22]);
        builder.setBolt("fooPartial25", new fooSleep("25"), 1).shuffleGrouping("fooPartial24").directGrouping("spout", PREPARE_STREAM_ID_list[23]);
        builder.setBolt("fooPartial26", new fooSleep("26"), 1).shuffleGrouping("fooPartial25").directGrouping("spout", PREPARE_STREAM_ID_list[24]);
        builder.setBolt("fooPartial27", new fooSleep("27"), 1).shuffleGrouping("fooPartial26").directGrouping("spout", PREPARE_STREAM_ID_list[25]);
        builder.setBolt("fooPartial28", new fooSleep("28"), 1).shuffleGrouping("fooPartial27").directGrouping("spout", PREPARE_STREAM_ID_list[26]);
        builder.setBolt("fooPartial29", new fooSleep("29"), 1).shuffleGrouping("fooPartial28").directGrouping("spout", PREPARE_STREAM_ID_list[27]);
        builder.setBolt("fooPartial30", new fooSleep("30"), 1).shuffleGrouping("fooPartial29").directGrouping("spout", PREPARE_STREAM_ID_list[28]);
        builder.setBolt("fooPartial31", new fooSleep("31"), 1).shuffleGrouping("fooPartial30").directGrouping("spout", PREPARE_STREAM_ID_list[29]);

        builder.setBolt("fooPartial32", new fooSleep("32"), 1).shuffleGrouping("fooPartial31").directGrouping("spout", PREPARE_STREAM_ID_list[30]);
        builder.setBolt("fooPartial33", new fooSleep("33"), 1).shuffleGrouping("fooPartial32").directGrouping("spout", PREPARE_STREAM_ID_list[31]);
        builder.setBolt("fooPartial34", new fooSleep("34"), 1).shuffleGrouping("fooPartial33").directGrouping("spout", PREPARE_STREAM_ID_list[32]);
        builder.setBolt("fooPartial35", new fooSleep("35"), 1).shuffleGrouping("fooPartial34").directGrouping("spout", PREPARE_STREAM_ID_list[33]);
        builder.setBolt("fooPartial36", new fooSleep("36"), 1).shuffleGrouping("fooPartial35").directGrouping("spout", PREPARE_STREAM_ID_list[34]);
        builder.setBolt("fooPartial37", new fooSleep("37"), 1).shuffleGrouping("fooPartial36").directGrouping("spout", PREPARE_STREAM_ID_list[35]);
        builder.setBolt("fooPartial38", new fooSleep("38"), 1).shuffleGrouping("fooPartial37").directGrouping("spout", PREPARE_STREAM_ID_list[36]);
        builder.setBolt("fooPartial39", new fooSleep("39"), 1).shuffleGrouping("fooPartial38").directGrouping("spout", PREPARE_STREAM_ID_list[37]);
        builder.setBolt("fooPartial40", new fooSleep("40"), 1).shuffleGrouping("fooPartial39").directGrouping("spout", PREPARE_STREAM_ID_list[38]);
        builder.setBolt("fooPartial41", new fooSleep("41"), 1).shuffleGrouping("fooPartial40").directGrouping("spout", PREPARE_STREAM_ID_list[39]);

        builder.setBolt("fooPartial42", new fooSleep("42"), 1).shuffleGrouping("fooPartial41").directGrouping("spout", PREPARE_STREAM_ID_list[40]);
        builder.setBolt("fooPartial43", new fooSleep("43"), 1).shuffleGrouping("fooPartial42").directGrouping("spout", PREPARE_STREAM_ID_list[41]);
        builder.setBolt("fooPartial44", new fooSleep("44"), 1).shuffleGrouping("fooPartial43").directGrouping("spout", PREPARE_STREAM_ID_list[42]);
        builder.setBolt("fooPartial45", new fooSleep("45"), 1).shuffleGrouping("fooPartial44").directGrouping("spout", PREPARE_STREAM_ID_list[43]);
        builder.setBolt("fooPartial46", new fooSleep("46"), 1).shuffleGrouping("fooPartial45").directGrouping("spout", PREPARE_STREAM_ID_list[44]);
        builder.setBolt("fooPartial47", new fooSleep("47"), 1).shuffleGrouping("fooPartial46").directGrouping("spout", PREPARE_STREAM_ID_list[45]);
        builder.setBolt("fooPartial48", new fooSleep("48"), 1).shuffleGrouping("fooPartial47").directGrouping("spout", PREPARE_STREAM_ID_list[46]);
        builder.setBolt("fooPartial49", new fooSleep("49"), 1).shuffleGrouping("fooPartial48").directGrouping("spout", PREPARE_STREAM_ID_list[47]);
        builder.setBolt("fooPartial50", new fooSleep("50"), 1).shuffleGrouping("fooPartial49").directGrouping("spout", PREPARE_STREAM_ID_list[48]);
        builder.setBolt("fooPartial51", new fooSleep("51"), 1).shuffleGrouping("fooPartial50").directGrouping("spout", PREPARE_STREAM_ID_list[49]);




        //////////////////////////

        //////////////////////////





////        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("fooPartial8");
        builder.setBolt("sink", new fooSink(sinkLogFileName), 1).shuffleGrouping("fooPartial51")
                .directGrouping("spout", PREPARE_STREAM_ID_list[50]);

        Config conf = new Config();
        conf.setNumAckers(1);
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE,false);
        conf.put(Config.TOPOLOGY_DEBUG, false);
        conf.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL, 10); //FIXME:AS4
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, new Integer(1048576));
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, new Integer(1048576));
        conf.put(Config.TOPOLOGY_STATE_PROVIDER,"org.apache.storm.redis.state.RedisKeyValueStateProvider");

        //        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,30); // in sec.
//        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, new Integer(32));
//        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS,5);


        if (argumentClass.getDeploymentMode().equals("C")) {
//            conf.setNumWorkers(6);
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, builder.createTopology());
        }

        else {
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


//    L   IdentityTopology   /Users/anshushukla/Downloads/Incomplete/stream/PStormScheduler/src/test/java/operation/output/eventDist.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp      /Users/anshushukla/Downloads/Incomplete/stream/iot-bm-For-Scheduler/modules/tasks/src/main/resources/tasks.properties  test