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
package org.apache.storm.topology;

import com.google.common.base.Stopwatch;
import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.spout.OurCheckpointSpout;
import org.apache.storm.state.State;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.storm.spout.CheckPointState.Action;
import static org.apache.storm.spout.CheckPointState.Action.*;

/**
 * Wraps a {@link IStatefulBolt} and manages the state of the bolt.
 */
public class StatefulBoltExecutor<T extends State> extends BaseStatefulBoltExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulBoltExecutor.class);
    private final IStatefulBolt<T> bolt;
    private State state;
    private boolean boltInitialized = false;
    private List<Tuple> pendingTuples = new ArrayList<>();
    private List<Tuple> preparedTuples = new ArrayList<>();
    private AckTrackingOutputCollector collector;

    public StatefulBoltExecutor(IStatefulBolt<T> bolt) {
        this.bolt = bolt;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // get the last successfully committed state from state store
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId();
        prepare(stormConf, context, collector, StateFactory.getState(namespace, stormConf, context));
    }

    // package access for unit tests
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector, State state) {
        init(context, collector);
        this.collector = new AckTrackingOutputCollector(collector);
        bolt.prepare(stormConf, context, this.collector);
        this.state = state;
        //FIXME:AS11 create a new file to send a RECOVERSTATE msg (for sending init msg)

        //FIXME:rebalance-sleep  create file

//        File file = new File(Config.BASE_SIGNAL_DIR_PATH +"REB_WAIT_DONE-"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
//        try {
//            if(file.createNewFile()) {
////                System.out.println("File_creation_successfull");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }

    @Override
    public void cleanup() {
        bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        bolt.declareOutputFields(declarer);
        declareCheckpointStream(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return bolt.getComponentConfiguration();
    }


    @Override
    protected void handleCheckpoint(Tuple checkpointTuple, Action action, long txid) {
        LOG.debug("handleCheckPoint with tuple {}, action {}, txid {}", checkpointTuple, action, txid);
        OurCheckpointSpout.logTimeStamp("HandleCheckpointST-" + Thread.currentThread() + "," + action + "," + System.currentTimeMillis());
        if (action == PREPARE) {
            if (boltInitialized) {
                Stopwatch stopwatch1 = Stopwatch.createStarted();//extra
                bolt.prePrepare(txid);// may need to do this on COMMIT msg
                stopwatch1.stop(); // optional
                OurCheckpointSpout.logTimeStamp("prePrepare_Stopwatch," + Thread.currentThread() + "," + stopwatch1.elapsed(MILLISECONDS));

//                Stopwatch stopwatch2 = Stopwatch.createStarted();//extra
//                state.prepareCommit(txid);
//                stopwatch2.stop(); // optional
//                preparedTuples.addAll(collector.ackedTuples());
//                OurCheckpointSpout.logTimeStamp("prepareCommit_Stopwatch," + Thread.currentThread() + "," + stopwatch2.elapsed(MILLISECONDS));
            } else {
                /*
                 * May be the task restarted in the middle and the state needs be initialized.
                 * Fail fast and trigger recovery.
                  */
                LOG.debug("Failing checkpointTuple, PREPARE received when bolt state is not initialized.");
                collector.fail(checkpointTuple);
                return;
            }
        } else if (action == COMMIT) {
            Stopwatch stopwatch1 = Stopwatch.createStarted();
            bolt.preCommit(txid);  // put to internal state (not to redis)
            stopwatch1.stop();
            OurCheckpointSpout.logTimeStamp("preCommit_Stopwatch," + Thread.currentThread() + "," + stopwatch1.elapsed(MILLISECONDS));

            Stopwatch stopwatch2 = Stopwatch.createStarted();//moved from prepare logic
            state.prepareCommit(txid); // written to redis as prepared state
            preparedTuples.addAll(collector.ackedTuples());
            stopwatch2.stop();
            OurCheckpointSpout.logTimeStamp("prepareCommit_Stopwatch," + Thread.currentThread() + "," + stopwatch2.elapsed(MILLISECONDS));

            Stopwatch stopwatch3 = Stopwatch.createStarted();//extra
            state.commit(txid);   // written to redis as commited state
            ack(preparedTuples);
            stopwatch3.stop();
            OurCheckpointSpout.logTimeStamp("commit_Stopwatch," + Thread.currentThread() + "," + stopwatch3.elapsed(MILLISECONDS));
        } else if (action == ROLLBACK) {
//            System.out.println("\n\n\n\n\t\t\t\t\tTEST_ENTERED_IN_ROLLBACK_STATE##########");//FIXME:SYSO REMOVED
            bolt.preRollback();
            state.rollback();
            fail(preparedTuples);
            fail(collector.ackedTuples());
        } else if (action == INITSTATE) {
//            System.out.println("TEST_boltInitialized"+boltInitialized);//FIXME:SYSO REMOVED


//            bolt.initState((T) state);
            if (!boltInitialized) {
                OurCheckpointSpout.logTimeStamp("INIT_RECEIVED," + Thread.currentThread() + "," + System.currentTimeMillis());
                boltInitialized = true;
                collector.delegate.ack(checkpointTuple);
                bolt.initState((T) state);

                LOG.debug("{} pending tuples to process", pendingTuples.size());
                for (Tuple tuple : pendingTuples) {
                    doExecute(tuple);
                }
                pendingTuples.clear();


                // FIXME:AS9 Ordering to be changed : OLD ordering
//                bolt.initState((T) state);
//                boltInitialized = true;
//                LOG.debug("{} pending tuples to process", pendingTuples.size());
//                for (Tuple tuple : pendingTuples) {
//                    doExecute(tuple);
//                }
//                pendingTuples.clear();
//                collector.delegate.ack(checkpointTuple);
            } else {
                LOG.debug("Bolt state is already initialized, ignoring tuple {}, action {}, txid {}",
                        checkpointTuple, action, txid);
//                System.out.println("TEST_Bolt_state_is_already_initialized, ignoring tuple {}, action {}, txid {}"+
//                        checkpointTuple+ action+txid);//FIXME:SYSO REMOVED
            }
        }
        //FIXME:AS5 if msg is on preparestream (or PREPARE msg), only ack it
        //FIXME:AS5 else frwrd to downstream (COMMIT msg)
//        OurCheckpointSpout.logTimeStamp("HandleCheckpointEND-" + Thread.currentThread() + "," + action + "," + System.currentTimeMillis());
//        System.out.println("TEST_action_for_current_msg_"+action);//FIXME:SYSO REMOVED
        if(action==COMMIT){
//            System.out.println("TEST_emitting_msg_on_CHECKPOINT_STREAM_ID_"+action);//FIXME:SYSO REMOVED
            collector.emit(CheckpointSpout.CHECKPOINT_STREAM_ID, checkpointTuple, new Values(txid, action));
            collector.delegate.ack(checkpointTuple);
        } else if (action == PREPARE) {
//            System.out.println("TEST_only_acking_OTHER_msg_not_emitting");//FIXME:SYSO REMOVED
            collector.delegate.ack(checkpointTuple);
        }
        OurCheckpointSpout.logTimeStamp("HandleCheckpointEND-" + Thread.currentThread() + "," + action + "," + System.currentTimeMillis()+","+checkpointTuple.toString());//FIXME: ERIC init logging at bolt
//        System.out.println("TEST_LOG_CHKPT_ACK_FROM_STATE_EXEC:"+Thread.currentThread()+"_"+action+","+System.currentTimeMillis());//FIXME:SYSO REMOVED
//        if(action.name().equals("PREPARE")){
//            System.out.println("TEST_only_acking_PREPARE_msg_not_emitting");
//            collector.delegate.ack(checkpointTuple);
//        }
//        if(action.name().equals("INITSTATE")){
//            System.out.println("TEST_only_acking_INITSTATE_msg_not_emitting");
//            collector.delegate.ack(checkpointTuple);
//        }
//        else {
//            collector.emit(CheckpointSpout.CHECKPOINT_STREAM_ID, checkpointTuple, new Values(txid, action));
//            collector.delegate.ack(checkpointTuple);
//        }

//        System.out.println("TEST_handleCheckpoint_ack_called_EXEC:"+action);//FIXME:SYSO REMOVED
    }

    @Override
    protected void handleTuple(Tuple input) {
        if (boltInitialized) {
            doExecute(input);
        } else {
            LOG.debug("Bolt state not initialized, adding tuple {} to pending tuples", input);
//            System.out.println("TEST_Bolt_state_not_initialized  adding tuple {} to pending tuples"+ input.toString()); //FIXME:SYSO REMOVED
            pendingTuples.add(input);
        }
    }

    private void doExecute(Tuple tuple) {
        bolt.execute(tuple);
    }

    private void ack(List<Tuple> tuples) {
        if (!tuples.isEmpty()) {
            LOG.debug("Acking {} tuples", tuples.size());
            for (Tuple tuple : tuples) {
                collector.delegate.ack(tuple);
            }
            tuples.clear();
        }
    }

    private void fail(List<Tuple> tuples) {
        if (!tuples.isEmpty()) {
            LOG.debug("Failing {} tuples", tuples.size());
            for (Tuple tuple : tuples) {
                collector.fail(tuple);
            }
            tuples.clear();
        }
    }

    private static class AckTrackingOutputCollector extends AnchoringOutputCollector {
        private final OutputCollector delegate;
        private final Queue<Tuple> ackedTuples;

        AckTrackingOutputCollector(OutputCollector delegate) {
            super(delegate);
            this.delegate = delegate;
            this.ackedTuples = new ConcurrentLinkedQueue<>();
        }

        List<Tuple> ackedTuples() {
            List<Tuple> result = new ArrayList<>();
            Iterator<Tuple> it = ackedTuples.iterator();
            while(it.hasNext()) {
                result.add(it.next());
                it.remove();
            }
            return result;
        }


        //A.S. called by user bolt logic
        @Override
        public void ack(Tuple input) {
//            System.out.println("TEST_adding_acking_tuple_to_ackedTuples"+input.toString());//FIXME:SYSO REMOVED
            ackedTuples.add(input);
        }
    }
}
