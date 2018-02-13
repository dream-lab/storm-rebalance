package storm.starter;


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



    public  boolean preExecute(Tuple in) {
        return true;
    }


    @Override
    public void preCommit(long txid) {
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
    }

}
