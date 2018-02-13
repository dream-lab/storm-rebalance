package org.apache.storm.starter.spout;


import org.apache.storm.spout.OurCheckpointSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.genevents.EventGen;
import org.apache.storm.starter.genevents.ISyntheticEventGen;
import org.apache.storm.starter.genevents.logging.BatchedFileLogging;
import org.apache.storm.starter.genevents.utils.GlobalConstants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SampleSpoutWithCHKPTSpout extends OurCheckpointSpout implements ISyntheticEventGen {
	SpoutOutputCollector _collector;
	EventGen eventGen;
	BlockingQueue<List<String>> eventQueue;
	String csvFileName;
	String outSpoutCSVLogFileName;
	String experiRunId;
	double scalingFactor;
	BatchedFileLogging ba;
	long msgId;

//	public static int val=0;


	public SampleSpoutWithCHKPTSpout(){
//		this.csvFileName = "/home/ubuntu/sample100_sense.csv";
//		System.out.println("Inside  sample spout code");
		this.csvFileName = "/home/tarun/j2ee_workspace/eventGen-anshu/eventGen/bangalore.csv";
		this.scalingFactor = GlobalConstants.accFactor;
//		System.out.print("the output is as follows");
	}

	public SampleSpoutWithCHKPTSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public SampleSpoutWithCHKPTSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
//		System.out.println("SampleSpout PID,"+ ManagementFactory.getRuntimeMXBean().getName());
		BatchedFileLogging.writeToTemp(this,this.outSpoutCSVLogFileName);
		Random r=new Random();


		try {
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup1")==0)
//				msgId= (long) (1*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup2")==0)
//				msgId= (long) (2*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup3")==0)
//				msgId= (long) (3*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup4")==0)
//				msgId= (long) (4*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup5")==0)
//				msgId= (long) (5*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup6")==0)
//				msgId= (long) (6*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup7")==0)
//				msgId= (long) (7*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup8")==0)
//				msgId= (long) (8*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup9")==0)
//				msgId= (long) (9*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup10")==0)
//				msgId= (long) (10*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup11")==0)
//				msgId= (long) (11*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup12")==0)
//				msgId= (long) (12*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup13")==0)
//				msgId= (long) (13*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup14")==0)
//				msgId= (long) (14*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup15")==0)
//				msgId= (long) (15*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup16")==0)
//				msgId= (long) (16*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup17")==0)
//				msgId= (long) (17*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup18")==0)
//				msgId= (long) (18*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup19")==0)
//				msgId= (long) (19*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup20")==0)
//				msgId= (long) (20*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup21")==0)
//				msgId= (long) (21*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup22")==0)
//				msgId= (long) (22*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup23")==0)
//				msgId= (long) (23*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup24")==0)
//				msgId= (long) (24*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
//			if(InetAddress.getLocalHost().getHostName().compareTo("Anshus-MacBook-Pro.local")==0)
//				msgId= (long) (24*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshudreamD")==0)
				msgId= (long) (24*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,11))+r.nextInt(10));

			else
				msgId= (long) (r.nextInt(100)*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,11))+r.nextInt(10));

//			else
//					msgId=r.nextInt(10000);


		} catch (UnknownHostException e) {

			e.printStackTrace();
		}


//		System.out.println("TOPOLOGY_NAME_"+map.get(Config.TOPOLOGY_NAME));


//		msgId=r.nextInt(10000);
		_collector = collector;
		super.open(map,context,collector);
		this.eventGen = new EventGen(this,this.scalingFactor);
		this.eventQueue = new LinkedBlockingQueue<List<String>>();
		String uLogfilename=this.outSpoutCSVLogFileName+msgId;
		this.eventGen.launch(this.csvFileName, uLogfilename); //Launch threads

		ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());


	}


	@Override
	public void nextTuple() {
		//** CHKPT logic start
//		val+=1;
//		if(val>10000 ) {
//			System.out.println("doemit_set_to_FALSE...");
//			doemit=false; // set flag to pause spout
//
//
////			Map conf = Utils.readStormConfig();
////			NimbusClient client = NimbusClient.getConfiguredClient(conf);
////
////			try {
////				client.getClient().activate("test");
////			} catch (TException e) {
////				e.printStackTrace();
////			}
////			System.out.println("Successfully submit command activate " + "test");
//		}
//
//		if(val==1) // for INITilisation
//			super.nextTuple(false);
//
//		if(!doemit) {
//			super.nextTuple(doemit);
//			return;
//		}


		if (isPaused()) {
			//			isPausedFlag=1;
			OurCheckpointSpout.logTimeStamp("LAST_OLD_APP_FOR_SINK_MSGID," + msgId);
			return;
		}
		//** CHKPT logic stop

		// TODO Auto-generated method stub
//		try {
//		System.out.println("spout Queue count= "+this.eventQueue.size());
		// allow multiple tuples to be emitted per next tuple.
		// Discouraged? https://groups.google.com/forum/#!topic/storm-user/SGwih7vPiDE
//		int count = 0, MAX_COUNT=10; // FIXME?
//		while(count < MAX_COUNT) {
		List<String> entry = this.eventQueue.poll(); // nextTuple should not block!

		if (entry == null) {
//			System.out.println("No_entry_in_queue");
            return;
		}
//			count++;
		Values values = new Values();
		StringBuilder rowStringBuf = new StringBuilder();
		for (String s : entry) {
			rowStringBuf.append(",").append(s);
		}
		String rowString = rowStringBuf.toString().substring(1);
//			String rowString = rowStringBuf.toString();
		values.add(rowString);
		msgId++;
		values.add(Long.toString(msgId));
//		System.out.println("TEST_emitting_data_tuple_MSGID:" + msgId);

		this._collector.emit(new Values(val, System.currentTimeMillis() - (24 * 60 * 60 * 1000), msgId));
//			this._collector.emit("datastream", new Values(val, System.currentTimeMillis() - (24 * 60 * 60 * 1000), msgId));

//			this._collector.emit(values);
		try {
//				msgId++;
			if (val == 0)
				ba.batchLogwriter(System.currentTimeMillis(), "MSGID0," + msgId);
			else
				ba.batchLogwriter(System.currentTimeMillis(), "MSGID1," + msgId);
			//ba.batchLogwriter(System.nanoTime(),"MSGID," + msgId);
		} catch (Exception e) {
			e.printStackTrace();
		}
//			System.out.println("values by source are -" + values);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		}
	}



	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//List<String> fieldsList = EventGen.getHeadersFromCSV(csvFileName);
		//fieldsList.add("MSGID");
		//declarer.declare(new Fields(fieldsList));

//		declarer.declare(new Fields("RowString", "MSGID"));

//		declarer.declareStream("datastream", new Fields("value", "ts", "MSGID"));
		declarer.declare(new Fields("value", "ts", "MSGID"));
		super.declareOutputFields(declarer);

	}

	@Override
	public void receive(List<String> event) {
		// TODO Auto-generated method stub
		//System.out.println("Called IN SPOUT### ");
		try {
			this.eventQueue.put(event);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

//	L   IdentityTopology   /Users/anshushukla/Downloads/Incomplete/stream/PStormScheduler/src/test/java/operation/output/eventDist.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp      /Users/anshushukla/Downloads/Incomplete/stream/iot-bm-For-Scheduler/modules/tasks/src/main/resources/tasks.properties  test