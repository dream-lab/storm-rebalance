package org.apache.storm.starter.bolt.operation;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Created by anshushukla on 02/01/16.
 */
public class UserHandler extends DefaultHandler {
    boolean bFirstName = false;
    boolean bLastName = false;
    boolean bNickName = false;
    boolean bMarks = false;
    public int total_length=0;

    @Override
    public void startElement(String uri,
                             String localName, String qName, Attributes attributes)
            throws SAXException {
        if (qName.equalsIgnoreCase("student")) {
//            System.out.println("START============================================================");
            String rollNo = attributes.getValue("rollno");
            total_length+=rollNo.length();
//            System.out.println("total_length rollno-"+total_length);
//            System.out.println("Roll No : " + rollNo);
        } else if (qName.equalsIgnoreCase("firstname")) {
            bFirstName = true;
            total_length+=5;
//            System.out.println("total_length firstname-"+total_length);
        } else if (qName.equalsIgnoreCase("lastname")) {
            bLastName = true;
            total_length+=6;
//            System.out.println("lastname-"+qName);

//            System.out.println("total_length lastname-"+total_length);
        } else if (qName.equalsIgnoreCase("nickname")) {
            bNickName = true;
            total_length+=7;
//            System.out.println("total_length nickname-"+total_length);
        }
        else if (qName.equalsIgnoreCase("marks")) {
            bMarks = true;
            total_length+=8;
//            System.out.println("total_length marks-"+total_length);
        }
    }


    public void endElement(String uri,
                           String localName, String qName) throws SAXException {
        if (qName.equalsIgnoreCase("student")) {
            total_length+=qName.length();
//            System.out.println("total_length student-"+total_length);
        }
//        System.out.println("END============================================================");
    }
    @Override
    public void characters(char ch[],
                           int start, int length) throws SAXException {
        if (bFirstName) {
            total_length+=length+1;
//                    new String(ch, start, length).intern().length();
            bFirstName = false;
//            System.out.println("total_length characters bFirstName-"+total_length);
//            System.out.println("TEST123-"+new String(ch, start, length));
        } else if (bLastName) {
            total_length+=length+2;
//            System.out.println("total_length characters bLastName-"+total_length);
//                    new String(ch, start, length).intern().length();
            bLastName = false;
        } else if (bNickName) {
            total_length+=length+3;
//            System.out.println("total_length characters bNickName-"+total_length);
//                    new String(ch, start, length).intern().length();
            bNickName = false;
        } else if (bMarks) {
            total_length+=length+4;
//            System.out.println("total_length characters bMarks-"+total_length);
//                    new String(ch, start, length).intern().length();
            bMarks = false;
        }
    }

}
