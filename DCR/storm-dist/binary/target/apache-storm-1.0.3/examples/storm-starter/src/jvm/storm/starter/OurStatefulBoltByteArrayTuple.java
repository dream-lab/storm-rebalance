package storm.starter;

//import org.apache.commons.collections.map.HashedMap;

import org.apache.storm.Config;
import org.apache.storm.serialization.KryoTupleDeserializer;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

//import org.eclipse.jetty.util.ArrayQueue;

/**
 * Created by anshushukla on 28/02/17.
 */
public abstract class OurStatefulBoltByteArrayTuple<T,V> extends BaseStatefulBolt<KeyValueState<T, V>> {
    //FIXME: our declared vars start
    boolean commitFlag=false,drainDone=true,passThrough=false;
    private static final Object DRAIN_LOCK = new Object();

    private List<byte []> ourPendingTuples = new ArrayList();
    private List<byte []> ourOutTuples = new ArrayList();
    public List<byte []> redisTuples = new ArrayList();


    KeyValueState<T, V> kvstate;
    OutputCollector collector;

    TopologyContext _context;
    KryoTupleSerializer kts;
    KryoTupleDeserializer ktd;
    KryoValuesSerializer kvs;
    KryoValuesDeserializer kvd;
    Map p;
    public static Logger l;

    //FIXME: inQueue
//    DisruptorQueue.QueueMetrics in=q.new QueueMetrics();

    @Override
    public void prePrepare(long txid) {
        l.info("TEST:prePrepare:"+Thread.currentThread().toString());

        //synchronized (DRAIN_LOCK) {
        //p=new HashMap()
//            drainDone=false;
//            System.out.println("TEST_prePrepare_drainDone_FALSE"+drainDone);

//            commitFlag=true;
//            long startDrain=System.currentTimeMillis();
            /*do {
                try {
                    System.out.println("TEST:locked for next 1 sec.");
                    DRAIN_LOCK.wait(1); // race condition to be fixed (5 sec wait)
                    System.out.println("TEST:setting drainDone flag true");
                    drainDone=true;
                    return;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
//                while (System.currentTimeMillis() - startDrain < 30000) ;
            while(true);  */
//            drainDone=true;
        //}


    }


    public  boolean preExecute(Tuple in) { // logic for checking commit flag thing accumulate msg //    execute or store it
//    {
//        System.out.println("TEST:preExecute:"+Thread.currentThread().toString());
//        p.put(Config.TOPOLOGY_KRYO_FACTORY,"org.apache.storm.serialization.DefaultKryoFactory");
//        p.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION,true);
//        p.put(Config.TOPOLOGY_TUPLE_SERIALIZER,"org.apache.storm.serialization.types.ListDelegateSerializer");
//        p.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS,true);
//
//         kts=new KryoTupleSerializer(p,_context);
//        System.out.println("TEST_preExecute_drainDone_Flag_Value"+drainDone);
//            synchronized (DRAIN_LOCK) {
//                if (!drainDone) {
//                    ourPendingTuples.add(kts.serialize(in)); // custom constr
//                    System.out.println("TEST_preExecute_written_to_ourPendingTuples");
////                    DRAIN_LOCK.notify();
//                    return false;
//                }
//            }
        return true;
    }


    @Override
    public void preCommit(long txid) {
//        System.out.println("TEST:preCommit");
//        kvstate.put((T)"OUR_OUT_TUPLES",(V)ourOutTuples);
//        kvstate.put((T)"OUR_PENDING_TUPLES",(V)ourPendingTuples);
    }


    @Override
    public void initState(KeyValueState<T, V> state) {
        l.info("TEST:initState start");

        File file = new File(Config.BASE_SIGNAL_DIR_PATH +"INIT_CALLED_"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
        try {
            if(file.createNewFile()) {
                l.info("INIT_creation_successfull");
//                OurCheckpointSpout.logTimeStamp("INIT_CALLED_,"+System.currentTimeMillis());
            }
            else
                l.info("INIT_file_already_exists_in_specified_path");
        } catch (Exception e) {
            e.printStackTrace();
        }

//        p.put(Config.TOPOLOGY_KRYO_FACTORY,"org.apache.storm.serialization.DefaultKryoFactory");
//        p.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION,true);
//        p.put(Config.TOPOLOGY_TUPLE_SERIALIZER,"org.apache.storm.serialization.types.ListDelegateSerializer");
//        p.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS,true);
//        ktd=new KryoTupleDeserializer(p,_context);
//        kvd=new KryoValuesDeserializer(p);
//
//        kvstate=state;
//        ourOutTuples= (List<byte[]>) kvstate.get((T) "OUR_OUT_TUPLES", (V) new ArrayList<byte []>());
//        ourPendingTuples= (List<byte[]>) kvstate.get((T) "OUR_PENDING_TUPLES", (V) new ArrayList<byte []>());
//
//        System.out.println("TEST_restored_tuples_from_redis_ourOutTuples:"+ourOutTuples.size()+"ourPendingTuples:"+ourPendingTuples.size());
//
//        for (int i = 0; i < ourOutTuples.size(); i=i+2) {
//            try {
//                System.out.println("TEST_initState_ourOutTuples_TUPLE_"+ktd.deserialize(ourOutTuples.get(i)).toString());
//                System.out.println("TEST_initState_ourOutTuples_VALUE_"+kvd.deserialize(ourOutTuples.get(i+1)).toString());
//                collector.emit(ktd.deserialize(ourOutTuples.get(i)),kvd.deserialize(ourOutTuples.get(i+1)));// FIXME: WRONG ack and emit the tuple (Re-chek)
//                collector.ack(ktd.deserialize(ourOutTuples.get(i)));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        ourOutTuples.clear();
//
//        for (int i = 0; i < ourPendingTuples.size(); i++) {
//            try {
//                System.out.println("TEST_initState_ourPendingTuples_TUPLE_"+ktd.deserialize(ourPendingTuples.get(i)).toString());
//                execute(ktd.deserialize(ourPendingTuples.get(i)));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        ourPendingTuples.clear();
//
//
////        redisTuples = (List<byte[]>) kvstate.get((T)"redisTuples", (V) new ArrayList<byte[]>());
////        System.out.println("SIZE_OF_REDIS_TUPLES:"+redisTuples.size());
////        for(int i=0;i<redisTuples.size();i++){
////            System.out.println("Redis_entry:"+ new String(redisTuples.get(i)));
////        }
//
//        // send first
////        for (CustomPair tuple : ourOutTuples) {
////            collector.emit(tuple.input,tuple.output);// FIXME: WRONG ack and emit the tuple (Re-chek)
////            collector.ack(tuple.input);
////        }
////        ourOutTuples.clear();
//////        passThrough=false;
////
////        for (CustomPair tuple : ourPendingTuples) {
////            execute(tuple.input);
////        }
////        ourPendingTuples.clear();
//        System.out.println("TEST:initState finish");
    }

}
