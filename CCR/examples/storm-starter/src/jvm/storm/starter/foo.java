package storm.starter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * Created by anshushukla on 28/02/17.
 */
//public class foo extends OurStatefulBoltByteArrayTuple<String,List<OurCustomPair1>> {
public class foo extends OurStatefulBoltByteArrayTuple<String,List<byte[]>> {
//public class foo extends OurStatefulBoltByteArray<String,List<byte[]>> {
//public class foo extends OurStatefulBolt<String,List<Tuple>> {
//    private static final Logger LOG = LoggerFactory.getLogger(StatefulTopology.class);


//    KeyValueState<String, List<Object>> kvState;
//    long sum;
    ;
//    public static String traceVal;
    foo(String name) {
        this.name = name; // this is being used as key for storing internal state of bolt
//        List<byte []> name+"STATE";
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.out.println("TEST:prepare");
        this.collector = collector;
        _context=context;
    }



    @Override
    public void execute(Tuple input) {
//        TupleUtils.isTick(input);

        if(!preExecute(input)){
            return;
            }

//        Utils.sleep(5);
        // user code
//        traceVal+=name;
//        System.out.println("TEST_traceVal_"+traceVal);
        String upVal= (input.getValueByField("value")).toString()+name;
        System.out.println("TEST_OurStatefulBolt_execute-"+upVal);
//        Utils.sleep(5);
//        redisTuples.add(input.getValueByField("MSGID").toString().getBytes(Charset.forName("UTF-8")));
//        System.out.println("Added_tuples_to_redisTuples_"+input.getValueByField("MSGID"));
//        kvstate.put("redisTuples", redisTuples);
//        kvstate.put(name,sum);
        Values out=  new Values(upVal,input.getValueByField("MSGID").toString());
//        Utils.sleep(2000);
//        emit(input,out);
        collector.emit(out);
//        super.initState();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value","MSGID"));
    }


//    @Override
//    public void initState(KeyValueState<String, List<Tuple>> state) {
//         List<Tuple> ourOutTuples= (List<Tuple>) state.get("OUR_OUT_TUPLES");
//         List<Tuple> ourPendingTuples= (List<Tuple>) state.get("OUR_PENDING_TUPLES");
//        System.out.println("TEST: restored tuples from redis ourOutTuples:"+ourOutTuples.size()+"ourPendingTuples:"+ourPendingTuples.size());
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

