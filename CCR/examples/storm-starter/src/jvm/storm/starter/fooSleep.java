package storm.starter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by anshushukla on 28/02/17.
 */

public class fooSleep extends OurStatefulBoltByteArrayTuple<String,List<byte[]>> {

    String inputFileString=null;
    String name;
//    KeyValueState<String, List<Object>> kvState;
    long sum;
//    public static String traceVal;
    fooSleep(String name) {
        this.name = name;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        initLogger(LoggerFactory.getLogger("APP"));
        l.info("TEST:prepare");
        this.collector = collector;
        _context=context;

    }



    @Override
    public void execute(Tuple input) {
        if(!preExecute(input)){
            return;
            }

        // user code
        String upVal= (input.getValueByField("value")).toString();




//        for(int i=0;i<3;i++)
//            tot_length += Operations.doXMLparseOp(inputFileString);

//        redisTuples.add(input.getValueByField("MSGID").toString().getBytes(Charset.forName("UTF-8")));
//        l.info("Added_tuples_to_redisTuples_"+input.getValueByField("MSGID"));
//        kvstate.put("redisTuples", redisTuples);
        Values out=  new Values(upVal,input.getValueByField("MSGID").toString());
        Utils.sleep(100);
//        emit(input,out);
        collector.emit(out);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value","MSGID"));
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

