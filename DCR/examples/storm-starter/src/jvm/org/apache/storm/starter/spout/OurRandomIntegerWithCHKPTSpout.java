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
package org.apache.storm.starter.spout;

import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Emits a random integer and a timestamp value (offset by one day),
 * every 100 ms. The ts field can be used in tuple time based windowing.
 */
public class OurRandomIntegerWithCHKPTSpout extends CheckpointSpout {
    private static final Logger LOG = LoggerFactory.getLogger(OurRandomIntegerWithCHKPTSpout.class);
    private SpoutOutputCollector collector;
    private Random rand;
    private long msgId = 0;
    private static int val=0;



    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
        super.open(conf,context,collector);
    }

    @Override
    public void nextTuple() {
//        Boolean doemit = true;
//        super.nextTuple(doemit);
//        if(!doemit) return;

        Utils.sleep(2000);
        val+=1;
        if(val<=30) {
            System.out.println("TEST_emitting_data_tuple");
            collector.emit("datastream", new Values(val, System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
        }

        if(val==1)
            super.nextTuple();  // for initialising bolt

//        else
            if(val>=20 ) {
            System.out.println("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
            super.nextTuple();
            //TODO: call to nexttuple should be non blocking bcz ack will change state from PREPARE to COMMIT
//            val=0;
        }
    }


//    @Override
//    public void nextTuple() {
//        Utils.sleep(2000);
//        val+=1;
//        if(val<=30) {
//            System.out.println("TEST_emitting_data_tuple");
//            collector.emit("datastream", new Values(val, System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
//        }
////        else
//        if(val>=20 ) {
//            System.out.println("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
//            super.nextTuple();
//            //TODO: call to nexttuple should be non blocking bcz ack will change state from PREPARE to COMMIT
////            val=0;
//        }
//    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("value", "ts", "msgid"));

        declarer.declareStream("datastream", new Fields("value", "ts", "msgid"));
//        declarer.declareStream("chkptstream", new Fields("Column","MSGID"));
//        declarer.declareStream(CHECKPOINT_STREAM_ID, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ACK for msgId : " + msgId);
        if ( isCheckpointAckBymsgId(msgId)) {
            System.out.println("ACK for CHECKPOINT_STREAM_ID msgId:"+msgId);
            super.ack(msgId);
        }
        else{
            System.out.println("ACK_for_datastream_msgId:"+msgId);
        }

    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got FAIL for msgId : " + msgId);
        if (isCheckpointAckBymsgId(msgId)) {
            System.out.println("FAIL for CHECKPOINT_STREAM_ID msgId:"+msgId);
            super.fail(msgId);
        }
        else{
            System.out.println("FAIL for datastream msgId:"+msgId);
        }
    }
}
