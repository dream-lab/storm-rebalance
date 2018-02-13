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

import org.apache.storm.utils.Utils;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Emits a random integer and a timestamp value (offset by one day),
 * every 100 ms. The ts field can be used in tuple time based windowing.
 */
public class OurCheckpointSpout extends CheckpointSpout {
    private static final Logger LOG = LoggerFactory.getLogger(OurCheckpointSpout.class);
    private SpoutOutputCollector collector;
//    private Random rand;
//    private long msgId = 0;
//    public static int val=0;
//    public boolean doemit = true;

    public int val=0;
    boolean haveLastCheckpointAck;
    boolean pause ;
    private static Logger l;
    public static void initLogger(Logger l_) {
        l = l_;
    }
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        initLogger(LoggerFactory.getLogger("APP"));
        super.open(conf,context,collector);
    }

    int valBound;
    public boolean isPaused() {
        l.info("*********************************************");
        // TODO logic to decide if we should pause
        pause = false;
        haveLastCheckpointAck=false;
        valBound+=1;

//        l.info("val_bound_and_pause_flag"+valBound+","+pause);
        if(valBound==1) // for INITilisation
            super.nextTuple();

        File f1 = new File(Config.BASE_SIGNAL_DIR_PATH +"RECOVERSTATE");
        if(f1.exists()){
            OurCheckpointSpout.logTimeStamp("RECOVERSTATE,"+System.currentTimeMillis());
            l.info("###########_Got_SIGNAL_to_RECOVERSTATE_###########");
            l.info("TEST:EMITTING_on_CHECKPOINT_STREAM just 1 init ......");
            recovering=true;
            recoveryStepInProgress=false;
	    Utils.sleep(1000);
            super.nextTuple();  //TODO: call to nexttuple should be non blocking (once started) bcz ack will change state from PREPARE to COMMIT
            /*l.info("deleting_RECOVERSTATE_file");
////            TODO:logic to delete file
            File recoverFile = new File(Config.BASE_SIGNAL_DIR_PATH +"RECOVERSTATE");

            if(recoverFile.delete()){
                l.info(recoverFile.getName() + " is deleted!");
            }else{
                l.info("Delete operation is failed.");
            }  */
        }

//        if(valBound>10  ) {
//            pause=true; // set flag to pause spout
//        }

        File f2 = new File(Config.BASE_SIGNAL_DIR_PATH +"STARTCHKPT");
        if(f2.exists()){
            logTimeStamp("STARTCHKPT,"+System.currentTimeMillis());
            l.info("###########_Got_SIGNAL_to_start_CHKPT_###########");
            pause=true; // set flag to pause spout
        }

        File folder = new File(Config.BASE_SIGNAL_DIR_PATH);
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles)
        {
            if (file.isFile())
            {
                String filename = file.getName().split("-")[0]; //split filename from it's extension
                if(filename.equals("LastCheckpointAck")) {
                    haveLastCheckpointAck = true;
                    break;
                }
            }
        }
        if(haveLastCheckpointAck){
            l.info("###########_Pausing_CHKPT_stream_also_###########");
            return true;
        }


        if(pause) {
            l.info("pausing_datastream_at_value"+valBound);
            l.info("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
            super.nextTuple();  //TODO: call to nexttuple should be non blocking (once started) bcz ack will change state from PREPARE to COMMIT
        }

        return pause;
    }
//
//    public void nextTuple(boolean doemit) {
//        if(!doemit) {
//            l.info("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
//            super.nextTuple();  //TODO: call to nexttuple should be non blocking (once started) bcz ack will change state from PREPARE to COMMIT
//            return;
//        }
//    }


//    @Override
//    public void nextTuple() {
////        Boolean doemit = true;
////        super.nextTuple(doemit);
////        if(!doemit) return;
//
//        Utils.sleep(2000);
//        val+=1;
//        if(val<=30) {
//            l.info("TEST_emitting_data_tuple");
////            collector.emit("datastream", new Values(val, System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
//        }
//            if(val>=20 ) {
//            l.info("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
//            super.nextTuple();
//            //TODO: call to nexttuple should be non blocking bcz ack will change state from PREPARE to COMMIT
//        }
//    }


//    @Override
//    public void nextTuple() {
//        Utils.sleep(2000);
//        val+=1;
//        if(val<=30) {
//            l.info("TEST_emitting_data_tuple");
//            collector.emit("datastream", new Values(val, System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
//        }
////        else
//        if(val>=20 ) {
//            l.info("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
//            super.nextTuple();
//            //TODO: call to nexttuple should be non blocking bcz ack will change state from PREPARE to COMMIT
////            val=0;
//        }
//    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declareStream("datastream", new Fields("value", "ts", "msgid"));
        declarer.declareStream(CHECKPOINT_STREAM_ID, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ACK for msgId : " + msgId);
            l.info("ACK_for_CHECKPOINT_STREAM_ID msgId:"+msgId);
//        OurCheckpointSpout.logTimeStamp("ACK_OurCheckpointSpout_RECVD"+msgId+","+System.currentTimeMillis());
            super.ack(msgId);
            if(CheckPointState.LastCheckpointAck) {
                OurCheckpointSpout.logTimeStamp("ACK_COMMIT,"+System.currentTimeMillis());
                val=1;
                l.info("checkpointing_is_done_check_file" + CheckPointState.LastCheckpointAck);
                CheckPointState.LastCheckpointAck=false;
                File file = new File(Config.BASE_SIGNAL_DIR_PATH +"LastCheckpointAck-"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
                try {
                    if(file.createNewFile()) {
                        l.info("File_creation_successfull");
                    }
                    else
                        l.info("Error_while_creating_File,file_already_exists_in_specified_path");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // code for ack for INIT msg
            if(CheckPointState.LastINITAck) {
                OurCheckpointSpout.logTimeStamp("ACK_INIT,"+System.currentTimeMillis());
                l.info("TEST_LOG_INIT_ACK:"+"," + System.currentTimeMillis());
                l.info("INIT_is_done_check_file" + CheckPointState.LastINITAck);
                CheckPointState.LastINITAck=false;
                File file = new File(Config.BASE_SIGNAL_DIR_PATH +"FLUSH_REDIS_INITAck-"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
                try {
                    if(file.createNewFile()) {
                        l.info("File_LastINITAck_creation_successfull");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        //code for PREPARE msg ack
        if(CheckPointState.LastPREPAREAck) {
            OurCheckpointSpout.logTimeStamp("ACK_PREP,"+System.currentTimeMillis());
//                System.out.println("TEST_LOG_PREP_ACK:" + ","+System.currentTimeMillis());//FIXME:SYSO REMOVED
//                System.out.println("PREPARE_is_done_check_file" + CheckPointState.LastPREPAREAck);//FIXME:SYSO REMOVED
            CheckPointState.LastPREPAREAck=false;
            File file = new File(Config.BASE_SIGNAL_DIR_PATH +"PREPAck-"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
            try {
                if(file.createNewFile()) {
//                        System.out.println("File_LastPREPAck_creation_successfull");//FIXME:SYSO REMOVED
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got FAIL for msgId : " + msgId);
        if (isCheckpointAckBymsgId(msgId)) {
            l.info("FAIL_for_CHECKPOINT_STREAM_ID msgId:"+msgId);
            super.fail(msgId);
        }
        else{
            l.info("FAIL_for_datastream msgId:"+msgId);
        }
    }


    // used for logging only
    public static void  logTimeStamp(String s){
        try
        {
            String filename = Config.BASE_SIGNAL_DIR_PATH + "LOGTS-" + Thread.currentThread();
            FileWriter fw = new FileWriter(filename,true); //the true will append the new data
            fw.write(s+"\n");//appends the string to the file
            fw.close();
        }
        catch(IOException ioe)
        {
            System.err.println("IOException: " + ioe.getMessage());
        }
    }

}
