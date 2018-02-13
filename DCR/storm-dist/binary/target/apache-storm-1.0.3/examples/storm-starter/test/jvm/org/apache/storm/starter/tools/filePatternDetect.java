package org.apache.storm.starter.tools;

import org.apache.storm.Config;

import java.io.File;

/**
 * Created by anshushukla on 09/04/17.
 */
public class filePatternDetect {

    public static void main(String[] args) {
//        String regexBetaPilot = "^\\d+\\.\\d+\\.[BP]+\\.\\d+$";
//        File f3 = new File(Config.BASE_SIGNAL_DIR_PATH +"LastCheckpointAck-*");// FIX me: pattern
//        if(f3.exists()){
//            System.out.println("###########_Pausing_CHKPT_stream_also_###########");
//        }
//        else {
//            System.out.println("unable to detect pattern....");
//        }

        File folder = new File(Config.BASE_SIGNAL_DIR_PATH);
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles)
        {
            if (file.isFile())
            {
                String filename = file.getName().split("-")[0]; //split filename from it's extension
                System.out.println(filename);
            }
        }

    }
}
