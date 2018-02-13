package org.apache.storm.starter.bolt.operation;

//import in.dream_lab.genevents.config.LatencyConfig;
//import in.dream_lab.bm.stream_iot.storm.bolts.boltsUidai.LatencyConfig;
import org.apache.storm.starter.bolt.LatencyConfig;
import org.xml.sax.InputSource;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Created by anshushukla on 02/01/16.
 */
public class Operations {
    public static void main(String[] args) {
        String inputXMLpath="src/test/java/operation/tempSAX.xml";
        try {
            String testxmlString= LatencyConfig.readFileWithSize(inputXMLpath, StandardCharsets.UTF_8);
            System.out.println(testxmlString);
            Operations.doXMLparseOp(testxmlString);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        Operations.doPIOp();
    }

    // check  floating pt operation
    public static float doFloatOp(long millisec) {  //Not used Now
        long start=System.currentTimeMillis();
        float sum = 0;
        while(System.currentTimeMillis()-start<=millisec) {
//            long startTime = System.nanoTime();

            Random r = new Random(100000);
            float randNum = r.nextInt();
            for(int i=0;i<10;i++)
            sum+= randNum * randNum;

//            System.out.println(randNum);
//            long stopTime = System.nanoTime();
//            System.out.println((stopTime - startTime) / (1000000.0));
        }
        return sum;
    }

    // check  floating pt operation
    public static double doPIOp(int n) {
        double  i, j;     // Number of iterations and control variables
        double f;           // factor that repeats
        double pi = 1;

//        System.out.println("Approximation of the number PI through the Viete's series\n");
//        long starttime=System.nanoTime();

        for(i = n; i > 1; i--) {
            f = 2;
            for(j = 1; j < i; j++){
                f = 2 + Math.sqrt(f);
            }
            f = Math.sqrt(f);
            pi = pi * f / 2;
        }
        pi *= Math.sqrt(2) / 2;
        pi = 2 / pi;

//        System.out.println("\nAproximated value of PI = \n" + pi);
//        long stopTime = System.nanoTime();
//        System.out.println("CHECK2:"+(stopTime - starttime) / (1000.0));
    return pi;
    }

    public static int doXMLparseOp(String input) {
//        long startTime = System.nanoTime();
        UserHandler userhandler=new UserHandler();;
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            saxParser.parse(new InputSource(new StringReader(input)),userhandler);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        long stopTime = System.nanoTime();
//        System.out.println("time taken once (in millisec) - "+(stopTime - startTime)/(1000000.0));
//        System.out.println("userhandler.total_length-"+userhandler.total_length);
        return userhandler.total_length;
    }




}
