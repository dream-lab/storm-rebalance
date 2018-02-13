package storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.spout.RandomIntegerSpout;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * Created by anshushukla on 02/03/17.
 */
public class FooWithoutState  {

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple);
//            LOG.debug("Got tuple {}", tuple);
            System.out.println("Got tuple {}"+tuple);
            collector.emit( new Values(tuple.getValueByField("value")));

//            System.out.println(" TEST:Total tuples combining all Queues");
//            KeyedRoundRobinQueue krr=new KeyedRoundRobinQueue();
//            System.out.println(" TEST: Total tuples combining all Queues:"+krr.getSize());

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            ofd.declare(new Fields("value"));
        }

    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomIntegerSpout());
        builder.setBolt("printer1", new PrinterBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("printer2", new PrinterBolt(), 1).shuffleGrouping("printer1");

        Config conf = new Config();
//        conf.setNumWorkers(2);
        conf.setDebug(true);

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


}
