package org.apache.storm.starter.bolt;

//import in.dream_lab.bm.stream_iot.storm.bolts.boltsUidai.LatencyConfig;

//import in.dream_lab.bm.stream_iot.storm.bolts.boltsUidai.operation.Operations;
//import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
//import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;
import org.apache.storm.starter.bolt.operation.Operations;
import org.apache.storm.starter.genevents.logging.BatchedFileLogging;
import org.apache.storm.starter.genevents.utils.GlobalConstants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Identity_SAXparser extends BaseRichBolt {


    String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public Identity_SAXparser(String csvFileNameOutSink){
        this.csvFileNameOutSink = csvFileNameOutSink;
    }
    String inputFileString=null;

    OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        BatchedFileLogging.writeToTemp(this, csvFileNameOutSink);
        this.collector=outputCollector;
        GlobalConstants.createBoltIdentifyingFiles(topologyContext);
//        String inputXMLpath="modules/storm/src/main/java/in/dream_lab/bm/stream_iot/storm/bolts/boltsUidai/operation/tempSAX.xml";
//        String inputXMLpath="/home/anshu/data/storm/dataset/tempSAX.xml";
        String inputXMLpath="/Users/anshushukla/Downloads/Incomplete/stream/iot-bm-For-Scheduler/modules/tasks/src/main/resources/tempSAX.xml";

        try {
            inputFileString= LatencyConfig.readFileWithSize(inputXMLpath,StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        String rowString = input.getStringByField("RowString");
//        String msgId = input.getString(input.size()-1);
        String msgId = input.getStringByField("MSGID");

        int tot_length = 0;
//        long startTime = System.nanoTime();

        for(int i=0;i<3;i++)
            tot_length += Operations.doXMLparseOp(inputFileString);

//        long stopTime = System.nanoTime();
//        System.out.println("time taken3loop  (in millisec) - "+(stopTime - startTime)/(1000000.0));
//         collector.emit(new Values(rowString+","+tot_length,msgId ));
//        collector.emit(new Values(new StringBuffer(rowString).append(tot_length%2),msgId));
        collector.emit(new Values(rowString+tot_length%2,msgId));
//        System.out.println("rowString Identity_SAXparser-"+rowString+","+tot_length);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("Column","MSGID","time"));//addon
 outputFieldsDeclarer.declare(new Fields("RowString","MSGID"));
    }
}