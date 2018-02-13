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

import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.state.KeyValueState;
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

import static org.apache.storm.spout.CheckPointState.Action;
import static org.apache.storm.spout.CheckPointState.Action.COMMIT;
import static org.apache.storm.spout.CheckPointState.Action.PREPARE;
import static org.apache.storm.spout.CheckPointState.Action.ROLLBACK;
import static org.apache.storm.spout.CheckPointState.Action.INITSTATE;
/**
 * Wraps a {@link IStatefulBolt} and manages the state of the bolt.
 */
public class OurStatefulBoltExecutor<T extends State> extends BaseStatefulBoltExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(OurStatefulBoltExecutor.class);
    private final IStatefulBolt<T> bolt;
    private State state;
    private boolean boltInitialized = false;
    private List<Tuple> pendingTuples = new ArrayList<>();
    private List<Tuple> preparedTuples = new ArrayList<>();
    private AckTrackingOutputCollector collector;

    //FIXME: our declared vars start
    boolean commitFlag=false,drainDone=false;   Object drainLock;
    private List<Tuple> ourPendingTuples = new ArrayList<>();
    private List<Tuple> ourOutTuples = new ArrayList<>();
//    private Map<Tuple,Values> ourOutTuples = new HashMap();
//    RedisKeyValueState<String, String> keyValueState;


    public OurStatefulBoltExecutor(IStatefulBolt<T> bolt) {
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

//        System.out.println("CALLING_initState_inside_prepare");
//        bolt.initState((T) state);

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
        // FIXME: TEST
        ((KeyValueState)state).put("OUR_PENDING_TUPLES",1L);
        System.out.println("TEST: putting to redis");


        LOG.debug("handleCheckPoint with tuple {}, action {}, txid {}", checkpointTuple, action, txid);
        if (action == PREPARE) {
            System.out.println("TEST: inside PREPARE action");
            if (boltInitialized) {
                bolt.prePrepare(txid);
                state.prepareCommit(txid);
                preparedTuples.addAll(collector.ackedTuples());

                //FIXME 1.1: while(queue is not empty) add all tuples to  ourPendingTuples and CHKPT to redis
                commitFlag=true;
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
            System.out.println("TEST: inside COMMIT action");
            bolt.preCommit(txid);
            state.commit(txid);
            ack(preparedTuples);
            //FIXME 3.1: code for saving   ourOutTuples and ourPendingTuples to queue
            ((KeyValueState)state).put("OUR_OUT_TUPLES",ourOutTuples);
            ((KeyValueState)state).put("OUR_PENDING_TUPLES",ourPendingTuples);

            //..........
        } else if (action == ROLLBACK) {
            bolt.preRollback();
            state.rollback();
            fail(preparedTuples);
            fail(collector.ackedTuples());
        } else if (action == INITSTATE) {
            System.out.println("TEST: inside INITSTATE action");
            if (!boltInitialized) {
                bolt.initState((T) state);
                boltInitialized = true;
                LOG.debug("{} pending tuples to process", pendingTuples.size());
                for (Tuple tuple : pendingTuples) {
                    doExecute(tuple);
                }
                pendingTuples.clear();

                //FIXME 1.3:  ????????
                // FIXME remaining getting ourPendingTuples from redis checkpointed storage
                ourOutTuples= (List<Tuple>) ((KeyValueState)state).get("OUR_OUT_TUPLES");
                ourPendingTuples= (List<Tuple>) ((KeyValueState)state).get("OUR_PENDING_TUPLES");
                System.out.println("TEST: restored tuples from redis ourOutTuples:"+ourOutTuples.size()+"ourPendingTuples:"+ourPendingTuples.size());
                commitFlag=false;
                for (Tuple tuple : ourPendingTuples) {
                    doExecute(tuple);
                }
                ourPendingTuples.clear();
                for (Tuple tuple : ourOutTuples) {
                    doExecute(tuple);  // if these are un processed tuples
//                    collector.ack(tuple);// FIXME: in case we receive the processed tuple
                }
                ourOutTuples.clear();

            } else {
                LOG.debug("Bolt state is already initialized, ignoring tuple {}, action {}, txid {}",
                          checkpointTuple, action, txid);
            }
        }
        collector.emit(CheckpointSpout.CHECKPOINT_STREAM_ID, checkpointTuple, new Values(txid, action));
        collector.delegate.ack(checkpointTuple);
    }

    @Override
    protected void handleTuple(Tuple input) {
        System.out.println("TEST: inside hadle tuple method");
        //FIXME 1.2:
        if (boltInitialized && commitFlag) {
            ourPendingTuples.add(input);
        }
        else if (boltInitialized && !commitFlag) {
            doExecute(input);
        }
        else if(!boltInitialized) {
            LOG.debug("Bolt state not initialized, adding tuple {} to pending tuples", input);
            pendingTuples.add(input);
        }
    }

    private void doExecute(Tuple tuple) {
        bolt.execute(tuple);
        //FIXME 2.1: storing the executed message in output tuple list
        // first check for our checkpoint flag set and store to redis
        if(commitFlag) {
            ourOutTuples.add(tuple);// FIXME: not processed copy
        }
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
            ackedTuples.add(input);
        }
    }
}
