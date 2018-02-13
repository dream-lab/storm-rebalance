package storm.starter;

//import org.apache.commons.collections.map.HashedMap;

import com.google.common.base.Stopwatch;
import org.apache.storm.Config;
import org.apache.storm.serialization.KryoTupleDeserializer;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.spout.OurCheckpointSpout;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;

import java.io.File;
import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

//import org.eclipse.jetty.util.ArrayQueue;

/**
 * Created by anshushukla on 28/02/17.
 */
public abstract class OurStatefulBoltByteArrayTuple<T,V> extends BaseStatefulBolt<KeyValueState<T, V>> {
    private static final Object DRAIN_LOCK = new Object();
    public static Logger l;
    //    private List<byte []> ourOutTuples ;
    public List<byte[]> redisTuples = new ArrayList();
    public String name;
    //FIXME: our declared vars start
    boolean drainDone = true;
    KeyValueState<T, V> kvstate;
    OutputCollector collector;
    TopologyContext _context;
    KryoTupleSerializer kts;
    KryoTupleDeserializer ktd;
    KryoValuesSerializer kvs;
    KryoValuesDeserializer kvd;
    Map p = new HashMap();
    private List<byte[]> ourPendingTuples;
    private boolean kryoInit;


    private void initKryo() {
        if (kryoInit) return;
        p = new HashMap();
        p.put(Config.TOPOLOGY_KRYO_FACTORY, "org.apache.storm.serialization.DefaultKryoFactory");
        p.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);
        p.put(Config.TOPOLOGY_TUPLE_SERIALIZER, "org.apache.storm.serialization.types.ListDelegateSerializer");
        p.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
        kvs = new KryoValuesSerializer(p);
        kts = new KryoTupleSerializer(p, _context);
        ktd = new KryoTupleDeserializer(p, _context);
        kryoInit = true;
    }

    @Override
    public void prePrepare(long txid) {
        l.info("TEST:prePrepare:" + Thread.currentThread().toString());
            kryoInit = false;
            drainDone=false;
        l.info("TEST_prePrepare_drainDone_FALSE" + drainDone);
    }


    public  boolean preExecute(Tuple in) // logic for checking commit flag thing accumulate msg //    execute or store it
    {
        l.info("TEST:preExecute:" + Thread.currentThread().toString());
//        ourPendingTuples.add(kts.serialize(in)); // custom constr
        l.info("TEST_preExecute_drainDone_Flag_Value" + drainDone);
        if (!drainDone) {
            initKryo();
            ourPendingTuples.add(kts.serialize(in)); // custom constr
            l.info("TEST_preExecute_written_to_ourPendingTuples" + ourPendingTuples.size() + "," + _context.getThisComponentId());
            return false;
        }

        return true;
    }


    @Override
    public void preCommit(long txid) {
        l.info("TEST:preCommit");
        l.info("TEST_writing_tuples_to_redis_Bolt_" + _context.getThisComponentId() + ",preCommit_getThisTaskId," + _context.getThisTaskId());
        l.info("ourPendingTuples:" + ourPendingTuples + "NULL_CHECK" + ourPendingTuples.size());
        kvstate.put((T) "OUR_PENDING_TUPLESX", (V) ourPendingTuples);
        OurCheckpointSpout.logTimeStamp("preCommitNumTuples," + Thread.currentThread() + "," + ourPendingTuples.size());
        drainDone=true;
    }



    @Override
    public void initState(KeyValueState<T, V> state) {
        l.info("TEST_initState_start");
        drainDone = true;

        File file = new File(Config.BASE_SIGNAL_DIR_PATH +"INIT_CALLED_"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
        try {
            if(file.createNewFile()) {
                l.info("INIT_creation_successfull");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // FIXME: if state is null, then nothing to do.
        initKryo();

        Stopwatch stopwatch1 = Stopwatch.createStarted();//extra
        kvstate=state;
        ourPendingTuples= (List<byte[]>) kvstate.get((T) "OUR_PENDING_TUPLESX", (V) new ArrayList<byte []>());
        stopwatch1.stop(); // optional
        OurCheckpointSpout.logTimeStamp("initState_kvstate_get_Stopwatch," + Thread.currentThread() + "," + stopwatch1.elapsed(MILLISECONDS));

        l.info("TEST_restored_tuples_from_redis_for_bolt_" + _context.getThisComponentId() + "ourPendingTuples:" + ourPendingTuples.size()
                + ",initState_getThisTaskId," + _context.getThisTaskId());
        OurCheckpointSpout.logTimeStamp("initRestoredNumTuples," + Thread.currentThread() + "," + ourPendingTuples.size());

        Stopwatch stopwatch2 = Stopwatch.createStarted();//extra
        for (int i = 0; i < ourPendingTuples.size(); i++) {
            try {
                l.info("TEST_initState_ourPendingTuples_TUPLE_" + ktd.deserialize(ourPendingTuples.get(i)).toString());
                execute(ktd.deserialize(ourPendingTuples.get(i)));
                OurCheckpointSpout.logTimeStamp("RECOVER_MESSAGES,"+ktd.deserialize(ourPendingTuples.get(i)).getValueByField("MSGID"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        ourPendingTuples.clear();
        stopwatch2.stop(); // optional
        OurCheckpointSpout.logTimeStamp("initState_execute_loop_Stopwatch," + Thread.currentThread() + "," + stopwatch2.elapsed(MILLISECONDS));

        l.info("TEST_initState_finish");
    }
}
