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

    private static Logger l;
    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        initLogger(LoggerFactory.getLogger("APP"));
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
                bolt.prePrepare(txid);
                stopwatch1.stop(); // optional
                OurCheckpointSpout.logTimeStamp("prePrepare_Stopwatch," + Thread.currentThread() + "," + stopwatch1.elapsed(MILLISECONDS));

                Stopwatch stopwatch2 = Stopwatch.createStarted();//moved from prepare logic
                state.prepareCommit(txid);
                preparedTuples.addAll(collector.ackedTuples());
                stopwatch2.stop();
                OurCheckpointSpout.logTimeStamp("prepareCommit_Stopwatch," + Thread.currentThread() + "," + stopwatch2.elapsed(MILLISECONDS));
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
            bolt.preCommit(txid);
            stopwatch1.stop();
            OurCheckpointSpout.logTimeStamp("preCommit_Stopwatch," + Thread.currentThread() + "," + stopwatch1.elapsed(MILLISECONDS));

            Stopwatch stopwatch3 = Stopwatch.createStarted();//extra
            state.commit(txid);
            ack(preparedTuples);
            stopwatch3.stop();
            OurCheckpointSpout.logTimeStamp("commit_Stopwatch," + Thread.currentThread() + "," + stopwatch3.elapsed(MILLISECONDS));
        } else if (action == ROLLBACK) {
            bolt.preRollback();
            state.rollback();
            fail(preparedTuples);
            fail(collector.ackedTuples());
        } else if (action == INITSTATE) {
            l.info("TEST_boltInitialized"+boltInitialized);
            if (!boltInitialized) {
                OurCheckpointSpout.logTimeStamp("INIT_RECEIVED," + Thread.currentThread() + "," + System.currentTimeMillis());
                bolt.initState((T) state);
                boltInitialized = true;
                LOG.debug("{} pending tuples to process", pendingTuples.size());
                for (Tuple tuple : pendingTuples) {
                    doExecute(tuple);
                }
                pendingTuples.clear();
            } else {
                LOG.debug("Bolt state is already initialized, ignoring tuple {}, action {}, txid {}",
                          checkpointTuple, action, txid);
                l.info("TEST_Bolt_state_is_already_initialized, ignoring tuple {}, action {}, txid {}"+
                        checkpointTuple+ action+txid);
            }
        }
        collector.emit(CheckpointSpout.CHECKPOINT_STREAM_ID, checkpointTuple, new Values(txid, action));
        collector.delegate.ack(checkpointTuple);
        l.info("TEST_handleCheckpoint_ack_called_EXEC:"+action);
        OurCheckpointSpout.logTimeStamp("HandleCheckpointEND-" + Thread.currentThread() + "," + action + "," + System.currentTimeMillis());
    }

    @Override
    protected void handleTuple(Tuple input) {
        if (boltInitialized) {
            doExecute(input);
        } else {
            LOG.debug("Bolt state not initialized, adding tuple {} to pending tuples", input);
            l.info("TEST_Bolt_state_not_initialized  adding tuple {} to pending tuples"+ input.toString());
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
            l.info("TEST_adding_acking_tuple_to_ackedTuples"+input.toString());
            ackedTuples.add(input);
        }
    }
}
