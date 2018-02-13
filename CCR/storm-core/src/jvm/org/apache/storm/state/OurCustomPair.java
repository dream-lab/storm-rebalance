package org.apache.storm.state;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;



public class OurCustomPair  implements Serializable{/// FIXME: implements serilizable

    private static final long serialVersionUID = -5606842333916087978L;


    Tuple input;
    Values output; // values out ??
    boolean outFlag;

    public OurCustomPair(){}
    public OurCustomPair(Tuple in){
        System.out.println("TEST:CustomPair1");
        input=in;
        outFlag=false;
    }
    public OurCustomPair(Tuple in, Values out){
        System.out.println("TEST:CustomPair2");
        input=in;
        output=out;
        outFlag=true;
    }

//    private void writeObject(ObjectOutputStream o)
//            throws IOException {
//        System.out.println("TEST:writeObject");
//
//        o.writeObject(input);
//        o.writeBoolean(outFlag);
//        if(outFlag)
//            o.writeObject(output);
//    }
//
//    private void readObject(ObjectInputStream o)
//            throws IOException, ClassNotFoundException {
//        System.out.println("TEST:readObject");
//
//        input = (Tuple) o.readObject();
//        outFlag=o.readBoolean();
//        if(outFlag)
//            output = (Values) o.readObject();
//    }
}
