package storm.starter;

import org.apache.storm.starter.genevents.logging.BatchedFileLogging;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by anshushukla on 28/02/17.
 */

public class fooSink extends OurStatefulBoltByteArrayTuple<String,List<byte[]>> {

    private static final Logger LOG = LoggerFactory.getLogger("APP");

    OutputCollector collector;
    BatchedFileLogging ba;
    String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    String name;

public fooSink(String csvFileNameOutSink){
    this.csvFileNameOutSink = csvFileNameOutSink;
}

    //    fooSink(String name) {
//        this.name = name;
//    }
    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        initLogger(LoggerFactory.getLogger("APP"));
        l.info("TEST:prepare");
        this.collector = collector;
        _context=context;

        BatchedFileLogging.writeToTemp(this, this.csvFileNameOutSink);
        ba = new BatchedFileLogging(this.csvFileNameOutSink + "-" + System.currentTimeMillis(), context.getThisComponentId());
    }



    @Override
    public void execute(Tuple input) {
        if(!preExecute(input)){
            return;
            }


        // user code

        l.info("SINK_CALLED..." + input);

//        String msgId = input.getStringByField("MSGID")+","+input.getStringByField("value");
        String msgId = input.getStringByField("MSGID")+",MSGID"+input.getStringByField("value");
        try {
            ba.batchLogwriter(System.currentTimeMillis(),msgId);
// ba.batchLogwriter(System.currentTimeMillis(),msgId+","+exe_time);//addon
        } catch (Exception e) {
            e.printStackTrace();
        }



//        String upVal= (input.getValueByField("value")).toString()+name;
//        l.info("TEST_OurStatefulBolt_execute:"+upVal);
//        Utils.sleep(5);
//        redisTuples.add(input.getValueByField("MSGID").toString().getBytes(Charset.forName("UTF-8")));
//        l.info("Added_tuples_to_redisTuples_"+input.getValueByField("MSGID"));
//        kvstate.put("redisTuples", redisTuples);
//        Values out=  new Values(upVal,input.getValueByField("MSGID").toString());
//        Utils.sleep(2000);
//        emit(input,out);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }


//    @Override
//    public void initState(KeyValueState<String, List<Tuple>> state) {
//         List<Tuple> ourOutTuples= (List<Tuple>) state.get("OUR_OUT_TUPLES");
//         List<Tuple> ourPendingTuples= (List<Tuple>) state.get("OUR_PENDING_TUPLES");
//        l.info("TEST: restored tuples from redis ourOutTuples:"+ourOutTuples.size()+"ourPendingTuples:"+ourPendingTuples.size());
//        commitFlag=false;
//        for (Tuple tuple : ourPendingTuples) {
//            execute(tuple);
//        }
//        ourPendingTuples.clear();
//        for (Tuple tuple : ourOutTuples) {
//                    collector.emit(tuple);
//                    collector.ack(tuple);// FIXME: in case we receive the processed tuple
//        }
//        ourOutTuples.clear();
//    }

}

