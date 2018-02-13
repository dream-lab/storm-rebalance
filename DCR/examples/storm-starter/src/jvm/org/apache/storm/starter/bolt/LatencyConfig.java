package org.apache.storm.starter.bolt;


import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LatencyConfig {

//actual values

    // Latency for PacketExtractionBolt
    public static final int PACKET_EXTRACTION_LATENCY = 2770;

    // Latency for DedupCheckBolt
    public static final int DEDUP_CHECK_LATENCY  = 2400;

    // Latency for QualityCheckBolt
    public static final int QUALITY_CHECK_LATENCY = 10000;

    // Latency for PacketValidationBolt
    public static final int PACKET_VALIDATION_LATENCY = 2400;


    // Latency for BioDedupBolt
    public static final int ABIS_INSERTION_LATENCY = (int) 4.50;
    public static final int ABIS_IDENTIFY_DEDUP_LATENCY= 2400;

    // Latency for ManualDedupCheckBolt
    public static final int MANUAL_DEDUP_LATENCY = 4800;

    // Latency for AadharGenerationBolt
    public static final int POST_AADHAR_LATENCY = 800;


    public static String readFileWithSize(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding).intern();
    }




    public static String readFileforOp() {
         String rowStringforop = null;
        try {
            rowStringforop = LatencyConfig.readFileWithSize("/Users/anshushukla/Downloads/Incomplete/stream/PStormScheduler/src/test/java/operation/tempSAX.xml", StandardCharsets.UTF_8);
//            rowStringforop = LatencyConfig.readFileWithSize("src/file_1MB", StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.out.println("files not readable");
            e.printStackTrace();
        }
        return rowStringforop;
    }


    public static void doStringOp(String filecontent) {

        long startTime = System.nanoTime();
//Insert your logic here
            String upper = filecontent.toUpperCase();
        long stopTime = System.nanoTime();

        System.out.println((stopTime - startTime)/(1000000.0));
    }

    public static void main(String[] args) {
        String filecontent=readFileforOp();
//        for(int i=0;i<20;i++)
        long start=System.currentTimeMillis();
        while(System.currentTimeMillis()-start<=10000)
            doStringOp(filecontent);

    }

    public static void doStringOp(int i) {

    }
}


