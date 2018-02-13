package org.apache.storm.state;

import java.io.Serializable;


public class OurCustomPair1 implements Serializable{/// FIXME: implements serilizable

    private static final long serialVersionUID = -5606842333916087978L;


    int test;
    public OurCustomPair1(){
    }
    public OurCustomPair1(int _test){
        test=_test;
    }
}
