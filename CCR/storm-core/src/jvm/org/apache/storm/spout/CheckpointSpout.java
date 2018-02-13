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
package org.apache.storm.spout;


import org.apache.storm.Config;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.storm.spout.CheckPointState.Action;
import static org.apache.storm.spout.CheckPointState.State.COMMITTED;

/**
 * Emits checkpoint tuples which is used to save the state of the {@link org.apache.storm.topology.IStatefulComponent}
 * across the topology. If a topology contains Stateful bolts, Checkpoint spouts are automatically added
 * to the topology. There is only one Checkpoint task per topology.
 * Checkpoint spout stores its internal state in a {@link KeyValueState}.
 *
 * @see CheckPointState
 */
public class CheckpointSpout extends BaseRichSpout {
//    public static final String PREPARE_STREAM_ID = "$checkpoint";
    public static final String CHECKPOINT_STREAM_ID = "$checkpoint";
    public static final String CHECKPOINT_COMPONENT_ID = "$checkpointspout";
    public static final String CHECKPOINT_FIELD_TXID = "txid";
    public static final String CHECKPOINT_FIELD_ACTION = "action";
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointSpout.class);
    private static final String TX_STATE_KEY = "__state";
    private static Logger l;
    //    private boolean recoveryStepInProgress;
    public boolean recoveryStepInProgress;
    //    private boolean recovering;
    public boolean recovering;
    Map<String, List<String>> componentID_streamID_map;
    Map<String, List<Integer>> componentID_taskID_map;
    //    Map<Long, Pair<String, String>> msgID_streamAction_map = new HashMap();
    Map<Long, EmittedMsgDetails> msgID_streamAction_map = new HashMap();
    //    int _ackReceivedCount;
    Set<Integer> _ackReceivedTaskIDSet;
    long chkptMsgid = 0;
    private TopologyContext context;
    private SpoutOutputCollector collector;
    private long lastCheckpointTs;
    private int checkpointInterval;
    private int sleepInterval;
    private boolean checkpointStepInProgress;
    private KeyValueState<String, CheckPointState> checkpointState;
    private CheckPointState curTxState;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static boolean isCheckpoint(Tuple input) {
        //FIXME:AS7
        return CHECKPOINT_STREAM_ID.equals(input.getSourceStreamId()) || (input.getSourceStreamId().contains("PREPARE_STREAM_ID"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        initLogger(LoggerFactory.getLogger("APP"));
        open(context, collector, loadCheckpointInterval(conf), loadCheckpointState(conf, context));
    }

    // package access for unit test
    void open(TopologyContext context, SpoutOutputCollector collector,
              int checkpointInterval, KeyValueState<String, CheckPointState> checkpointState) {
        this.context = context;
        this.collector = collector;
        this.checkpointInterval = checkpointInterval;
        this.sleepInterval = checkpointInterval / 10;
        this.checkpointState = checkpointState;
        this.curTxState = checkpointState.get(TX_STATE_KEY);
        lastCheckpointTs = 0;
        recoveryStepInProgress = false;
        checkpointStepInProgress = false;
        recovering = true;

        _ackReceivedTaskIDSet = new HashSet<>();


    }

    @Override
    public void nextTuple() {
        l.info("TEST_CheckpointSpout_nextTuple:state:" + curTxState.getState().name());
        l.info("TEST_shouldRecover_recovering_recoveryStepInProgress" + shouldRecover() + "," + recovering + "," + recoveryStepInProgress);
        if (shouldRecover()) {
            l.info("TEST_handleRecovery");
            handleRecovery();
            startProgress();
        } else if (shouldCheckpoint()) {
            l.info("TEST_doCheckpoint");
            doCheckpoint();
            startProgress();
        } else {
            l.info("SLEEPING...");
            Utils.sleep(sleepInterval);
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ack with txid {}, current txState {}", msgId, curTxState);
        l.info("REWIRE_curTxState:" + curTxState + ",msgId:" + ((Number) msgId).longValue());
        l.info("REWIRE_msgID_streamAction_map_ACK," + msgId + "," + msgID_streamAction_map.get(msgId) + ",msgID_streamAction_map_size," + msgID_streamAction_map.size());
        l.info("REWIRE_custom_object_details," + msgID_streamAction_map.get(msgId).getstreamID() + ",taskID," + msgID_streamAction_map.get(msgId).gettaskID()
                + ",msgID," + msgID_streamAction_map.get(msgId).getchkptMsgid() + ",Action," + msgID_streamAction_map.get(msgId).getaction().name());

        _ackReceivedTaskIDSet.add(msgID_streamAction_map.get(msgId).gettaskID()); // to check that ACK is received from all taskIDs
        l.info("REWIRE_ackReceivedCount_taskids:" + _ackReceivedTaskIDSet + ",size," + _ackReceivedTaskIDSet.size() + ",getAllTaskCount:" + getAllTaskCount());

        OurCheckpointSpout.logTimeStamp("ACK_COUNTING," +
                msgID_streamAction_map.get(msgId).getstreamID() + ",taskID," + msgID_streamAction_map.get(msgId).gettaskID()
                + ",msgID," + msgID_streamAction_map.get(msgId).getchkptMsgid() + ",Action," + msgID_streamAction_map.get(msgId).getaction().name() + "," + System.currentTimeMillis());
        if (_ackReceivedTaskIDSet.size() == getAllTaskCount()) {
            OurCheckpointSpout.logTimeStamp("ACK_FINAL," +
                    msgID_streamAction_map.get(msgId).getstreamID() + ",taskID," + msgID_streamAction_map.get(msgId).gettaskID()
                    + ",msgID," + msgID_streamAction_map.get(msgId).getchkptMsgid() + ",Action," + msgID_streamAction_map.get(msgId).getaction().name() + "," + System.currentTimeMillis());
            l.warn("REWIRE_COUNT_is_equal....");
            if (recovering) {
                handleRecoveryAck();
            } else {
                handleCheckpointAck();
            }
            resetProgress();
            _ackReceivedTaskIDSet.clear();// reset the list for INIT acks
            msgID_streamAction_map.clear(); // clearing required for checking for COMMIT message after init and prepare phases
        } else if (msgID_streamAction_map.get(msgId).getaction().name().equals("COMMIT")) {
            OurCheckpointSpout.logTimeStamp("ACK_FINAL," +
                    msgID_streamAction_map.get(msgId).getstreamID() + ",taskID," + msgID_streamAction_map.get(msgId).gettaskID()
                    + ",msgID," + msgID_streamAction_map.get(msgId).getchkptMsgid() + ",Action," + msgID_streamAction_map.get(msgId).getaction().name() + "," + System.currentTimeMillis());
            l.warn("REWIRE_ack_for_COMMIT_msg");
            if (recovering) {
                handleRecoveryAck();
            } else {
                handleCheckpointAck();
            }
            resetProgress();
            _ackReceivedTaskIDSet.clear();
            msgID_streamAction_map.clear(); // clearing required for checking for COMMIT message after init and prepare phases
        }
//        System.out.println("REWIRE_ackReceivedCount_taskids:" + _ackReceivedTaskIDSet + ",getAllTaskCount:" + getAllTaskCount());
    }

    @Override
    public void fail(Object msgId) {

        EmittedMsgDetails emd = msgID_streamAction_map.get(msgId);
//        collector.emitDirect(taskID, streamID, new Values(txid, action), chkptMsgid);
        if (l.isWarnEnabled())
            l.warn("Emitting_failed_message," + emd.getstreamID() + ",TaskID," + emd.gettaskID() + "," + emd.getaction());
        collector.emitDirect(emd.gettaskID(), emd.getstreamID(), new Values(emd.gettxid(), emd.getaction()), msgId);

//        LOG.debug("Got fail with msgid {}", msgId);
//        if (!recovering) {
//            LOG.debug("Checkpoint failed, will trigger recovery");
//            recovering = true;
//        }
//        resetProgress();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CHECKPOINT_STREAM_ID, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
    }

    /**
     * Loads the last saved checkpoint state the from persistent storage.
     */
    private KeyValueState<String, CheckPointState> loadCheckpointState(Map conf, TopologyContext ctx) {
        String namespace = ctx.getThisComponentId() + "-" + ctx.getThisTaskId();
        KeyValueState<String, CheckPointState> state =
                (KeyValueState<String, CheckPointState>) StateFactory.getState(namespace, conf, ctx);
        if (state.get(TX_STATE_KEY) == null) {
            CheckPointState txState = new CheckPointState(-1, COMMITTED);
            state.put(TX_STATE_KEY, txState);
            state.commit();
            l.info("Initialized checkpoint spout state with txState {}" + txState);
            LOG.debug("Initialized checkpoint spout state with txState {}", txState);
        } else {
            l.info("Got checkpoint spout state {}" + state.get(TX_STATE_KEY));
            LOG.debug("Got checkpoint spout state {}", state.get(TX_STATE_KEY));
        }
        return state;
    }

    private int loadCheckpointInterval(Map stormConf) {
        int interval = 0;
        if (stormConf.containsKey(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)) {
            interval = ((Number) stormConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)).intValue();
        }
        // ensure checkpoint interval is not less than a sane low value.
        interval = Math.max(100, interval);
        LOG.info("Checkpoint interval is {} millis", interval);
        return interval;
    }

    private boolean shouldRecover() {
        return recovering && !recoveryStepInProgress;
    }

    private boolean shouldCheckpoint() {
        l.info("TEST_calling_shouldCheckpoint...");
        l.info("TEST_conditions:" + !recovering + "," + !checkpointStepInProgress + ",(" + curTxState.getState() + ",||" + checkpointIntervalElapsed() + ")");
//        return !recovering && !checkpointStepInProgress &&
//                (curTxState.getState() != COMMITTED || checkpointIntervalElapsed());
        //FIXME:AS3
        return !recovering && !checkpointStepInProgress ;  //  once commited do recover only
    }

    private boolean checkpointIntervalElapsed() {
        l.info("TEST_timer_" + (System.currentTimeMillis() - lastCheckpointTs) + ":" + checkpointInterval);
        return (System.currentTimeMillis() - lastCheckpointTs) > checkpointInterval;
    }

    private void handleRecovery() {
        LOG.debug("In recovery");
        Action action = curTxState.nextAction(true);
        emit(curTxState.getTxid(), action);
    }

    private void handleRecoveryAck() {
        CheckPointState nextState = curTxState.nextState(true);
        if (curTxState != nextState) {
            l.info("TEST_handleRecoveryAck_" + "equal_state" + curTxState);
            saveTxState(nextState);
        } else {
            LOG.debug("Recovery complete, current state {}", curTxState);
            l.info("TEST_Recovery_complete_setting_recovering_FALSE" + curTxState);
            recovering = false;
        }
    }

    private void doCheckpoint() {
        LOG.debug("In checkpoint");
        if (curTxState.getState() == COMMITTED) {
            saveTxState(curTxState.nextState(false));
            lastCheckpointTs = System.currentTimeMillis();
//            System.out.println("TEST_TIMER_STARTED.....");//FIXME:SYSO REMOVED
        }
        Action action = curTxState.nextAction(false);
//        System.out.println("TEST_CheckpointSpout_doCheckpoint:action:"+action.name());//FIXME:SYSO REMOVED
        emit(curTxState.getTxid(), action);
    }

    private void handleCheckpointAck() {
        CheckPointState nextState = curTxState.nextState(false);
        saveTxState(nextState);
//        System.out.println("TEST_CheckpointSpout_handleCheckpointAck_"+nextState);//FIXME:SYSO REMOVED
    }

    private void emit(long txid, Action action) {
//        OurCheckpointSpout.logTimeStamp(action + "_SENT," + System.currentTimeMillis()); // FIXME: ERIC logging with msgid for tracking init messages
        LOG.debug("Current state {}, emitting txid {}, action {}", curTxState, txid, action);
//        System.out.println("TEST_emitting_Current_state {}, emitting txid {}, action {}"+","+ curTxState+","+ txid+","+ action);//FIXME:SYSO REMOVED
        //FIXME:LOG1
//        System.out.println("TEST_LOG_emit_spout:"+context.getThisComponentId()+"_"+action+","+System.currentTimeMillis());//FIXME:SYSO REMOVED
        if(action.name().equals("COMMIT")) {
            l.info("TEST_Emitting_on_CHECKPOINT_STREAM_ID_ID");//FIXME:SYSO REMOVED
            chkptMsgid = chkptMsgid + 1;
            collector.emit(CHECKPOINT_STREAM_ID, new Values(txid, action), chkptMsgid);
            OurCheckpointSpout.logTimeStamp(action + "_SENT," + System.currentTimeMillis());

            msgID_streamAction_map.put(chkptMsgid, new EmittedMsgDetails(-101, "$checkpoint", txid, action, chkptMsgid)); // putting taskID
        }
        else {

            for (String boltName : componentID_taskID_map.keySet()) {
                for (int taskID : componentID_taskID_map.get(boltName)) {
                    List<String> streamIDList = componentID_streamID_map.get(boltName);
                    for (String streamID : streamIDList) {
                        if (!streamID.equals("$checkpoint") && !streamID.equals("default")) {
                            chkptMsgid = chkptMsgid + 1;
                            l.info("REWIRE_emitting_on_streamid:" + streamID + ",txid," + txid + ",chkptMsgid," + chkptMsgid + ",action," + action.name() + ",taskID," + taskID);
                            collector.emitDirect(taskID, streamID, new Values(txid, action), chkptMsgid);
                            OurCheckpointSpout.logTimeStamp(action + "_SENT_MSGID,"+chkptMsgid+","+taskID+"_"+streamID +","+ System.currentTimeMillis());
                            msgID_streamAction_map.put(chkptMsgid, new EmittedMsgDetails(taskID, streamID, txid, action, chkptMsgid));
                        }
                    }
                }
            }
            if(action.name().equals("INITSTATE"))  // FIXME:ERIC_TEST
                Utils.sleep(100);
            l.info("REWIRE_msgID_streamAction_map:" + msgID_streamAction_map);
//            System.out.println("TEST_Emitting_on_PREPARE_STREAM_ID");//FIXME:SYSO REMOVED
        }
    }

    private void saveTxState(CheckPointState txState) {
        LOG.debug("saveTxState, current state {} -> new state {}", curTxState, txState);
        checkpointState.put(TX_STATE_KEY, txState);
        checkpointState.commit();
        curTxState = txState;
    }

    private void startProgress() {
        if (recovering) {
            recoveryStepInProgress = true;
        } else {
            checkpointStepInProgress = true;
        }
    }

    private void resetProgress() {
        if (recovering) {
            l.info("setting_recoveryStepInProgress_false_on_ACK");
            recoveryStepInProgress = false;
        } else {
            checkpointStepInProgress = false;
        }
    }


//    //FIXME: extra dded by A.S.
    public boolean isCheckpointAckBymsgId(Object msgId){
        return (curTxState.getTxid() == ((Number) msgId).longValue()) ;
    }

    public int getAllTaskCount() {
        int _taskCount = 0;
        //total tasks other than spout
        for (String boltName : componentID_taskID_map.keySet()) {
            _taskCount += componentID_taskID_map.get(boltName).size();
        }
        return _taskCount;
    }

    public class EmittedMsgDetails {
        //        collector.emitDirect(taskID, streamID, new Values(txid, action), chkptMsgid);
        private int _taskID;
        private String _streamID;
        private long _txid;
        private Action _action;
        private long _chkptMsgid;

        EmittedMsgDetails(int taskID, String streamID, long txid, Action action, long chkptMsgid) {
            _taskID = taskID;
            _streamID = streamID;
            _txid = txid;
            _action = action;
            _chkptMsgid = chkptMsgid;
        }

        public int gettaskID() {
            return _taskID;
        }

        public String getstreamID() {
            return _streamID;
        }

        public long gettxid() {
            return _txid;
        }

        public Action getaction() {
            return _action;
        }

        public long getchkptMsgid() {
            return _chkptMsgid;
        }

    }
}
