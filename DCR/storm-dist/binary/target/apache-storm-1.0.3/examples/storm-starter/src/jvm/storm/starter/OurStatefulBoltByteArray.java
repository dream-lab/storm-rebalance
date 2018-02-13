package storm.starter;

//import org.apache.commons.collections.map.HashedMap;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.OurCustomPair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

//import org.eclipse.jetty.util.ArrayQueue;

/**
 * Created by anshushukla on 28/02/17.
 */
public abstract class OurStatefulBoltByteArray<T,V> extends BaseStatefulBolt<KeyValueState<T, V>> {
    //FIXME: our declared vars start
    boolean commitFlag=false,drainDone=false,passThrough=false;
    private static final Object DRAIN_LOCK = new Object();
    private List<OurCustomPair> ourPendingTuples = new ArrayList();
    private List<OurCustomPair> ourOutTuples = new ArrayList();
    private List<byte []> dummy = new ArrayList();


    KeyValueState<T, V> kvstate;
    OutputCollector collector;

    //FIXME: inQueue
//    DisruptorQueue q;
//    DisruptorQueue.QueueMetrics in=q.new QueueMetrics();

    @Override
    public void prePrepare(long txid) {
        System.out.println("TEST:prePrepare");

        synchronized (DRAIN_LOCK) {
            drainDone=false;
//            commitFlag=true;
//            long startDrain=System.currentTimeMillis();
            do {
                try {
                    DRAIN_LOCK.wait(5000); // race condition to be fixed (5 sec wait)
                    drainDone=true;
                    return;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
//                while (System.currentTimeMillis() - startDrain < 30000) ;
            while(true);


//            drainDone=true;
        }

    }


    public  boolean preExecute(Tuple in) // logic for checking commit flag thing accumulate msg
//    execute or store it
    {
        dummy.add(new byte[]{10,20,30});
        System.out.println("TEST:preExecute");
            synchronized (DRAIN_LOCK) {
                if (!drainDone) {
                    ourPendingTuples.add(new OurCustomPair(in)); // custom constr
                    //FIXME: remove dummy
//                    for(int i=0;i<10;i++)
//                    dummy.add(new byte[]{10,20,30});
                    System.out.println("TEST:preExecute written to dummy");
                    DRAIN_LOCK.notify();
                    return false;
                }
            }
        return true;
    }


    @Override
    public void preCommit(long txid) {
//        kvstate.put((T)"OUR_OUT_TUPLES",(V)ourOutTuples);
//        kvstate.put((T)"OUR_PENDING_TUPLES",(V)ourPendingTuples);
        System.out.println("TEST:preCommit");
        kvstate.put((T)"OUR_PENDING_TUPLES2",(V)dummy);
    }

    public  void emit(Tuple input, Values out) // used by user for emitting
    {
        System.out.println("TEST:emit");
//         logic for checking ack after post processing and then emit
        synchronized (DRAIN_LOCK) {
            if (!drainDone) {
                ourOutTuples.add(new OurCustomPair(input, out));
                DRAIN_LOCK.notify();
                return ;
            }
        }
                collector.emit(input,out);
                collector.ack(input);

    }







    @Override
    public void initState(KeyValueState<T, V> state) {
        System.out.println("TEST:initState start");
        System.out.println(state.getClass());
        kvstate=state;
        //FIXME: remove dummy code
        dummy= (List<byte[]>) kvstate.get((T) "OUR_PENDING_TUPLES2", (V) new ArrayList<byte []>());
        System.out.println("TEST:DUMMY size:"+dummy.size());
//        for(byte b:dummy.get(0)){
//            System.out.println("TEST:DUMMY data:"+b);
//        }

//         ourOutTuples= (List<CustomPair>) state.get((T) "OUR_OUT_TUPLES",  (V)new ArrayList<CustomPair>());
//         ourPendingTuples= (List<CustomPair>) state.get((T) "OUR_PENDING_TUPLES",(V) new ArrayList<CustomPair>());
        System.out.println("TEST: restored tuples from redis ourOutTuples:"+ourOutTuples.size()+"ourPendingTuples:"+ourPendingTuples.size());

        // send first
//        for (CustomPair tuple : ourOutTuples) {
//            collector.emit(tuple.input,tuple.output);// FIXME: WRONG ack and emit the tuple (Re-chek)
//            collector.ack(tuple.input);
//        }
//        ourOutTuples.clear();
////        passThrough=false;
//
//        for (CustomPair tuple : ourPendingTuples) {
//            execute(tuple.input);
//        }
//        ourPendingTuples.clear();
        System.out.println("TEST:initState finish");
    }



//    public class CustomPair implements Serializable{/// FIXME: implements serilizable
//
//        private static final long serialVersionUID = -5606842333916087978L;
//
//        Tuple input;
//        Values output; // values out ??
//        boolean outFlag;
//
//        CustomPair(){
//
//        }
//
//
//        CustomPair(Tuple in){
//            System.out.println("TEST:CustomPair1");
//            input=in;
//            outFlag=false;
//        }
//        CustomPair(Tuple in, Values out){
//            System.out.println("TEST:CustomPair2");
//            input=in;
//            output=out;
//            outFlag=true;
//        }
//
//        private void writeObject(ObjectOutputStream o)
//                throws IOException {
//            System.out.println("TEST:writeObject");
//
//            o.writeObject(input);
//            o.writeBoolean(outFlag);
//            if(outFlag)
//                o.writeObject(output);
//        }
//
//        private void readObject(ObjectInputStream o)
//                throws IOException, ClassNotFoundException {
//            System.out.println("TEST:readObject");
//
//            input = (Tuple) o.readObject();
//            outFlag=o.readBoolean();
//            if(outFlag)
//                output = (Values) o.readObject();
//        }
//    }

}
