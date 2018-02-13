package org.apache.storm.starter.bolt.operation;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;

/**
 * Created by anshushukla on 02/01/16.
 */

public class XMLparse {

    public static void readXMLDom(){
        try {

            File fXmlFile = new File("src/test/java/operation/temp.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);

            //optional, but recommended
            //read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
            doc.getDocumentElement().normalize();

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

            NodeList nList = doc.getElementsByTagName("staff");

            System.out.println("----------------------------");

            for (int temp = 0; temp < nList.getLength(); temp++) {

                Node nNode = nList.item(temp);

                System.out.println("\nCurrent Element :" + nNode.getNodeName());

                if (nNode.getNodeType() == Node.ELEMENT_NODE) {

                    Element eElement = (Element) nNode;

                    System.out.println("Staff id : " + eElement.getAttribute("id"));
                    System.out.println("First Name : " + eElement.getElementsByTagName("firstname").item(0).getTextContent());
                    System.out.println("Last Name : " + eElement.getElementsByTagName("lastname").item(0).getTextContent());
                    System.out.println("Nick Name : " + eElement.getElementsByTagName("nickname").item(0).getTextContent());
                    System.out.println("Salary : " + eElement.getElementsByTagName("salary").item(0).getTextContent());

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public static void readXmlSAX(File inputFile){
//        File inputFile=null;
        try {
//            if(new Random().nextInt()%2==0) {
//                System.out.println("even");
//                 inputFile = new File("src/test/java/operation/temp.xml");
//            }
//            else {
//                System.out.println("odd");
//                inputFile = new File("src/test/java/operation/tempSAX.xml");
//            }
//            inputFile = new File("src/test/java/operation/tempSAX.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            UserHandler userhandler = new UserHandler();
            saxParser.parse(inputFile, userhandler);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        class UserHandler extends DefaultHandler {
//
//            boolean bFirstName = false;
//            boolean bLastName = false;
//            boolean bNickName = false;
//            boolean bMarks = false;
//
//            @Override
//            public void startElement(String uri,
//                                     String localName, String qName, Attributes attributes)
//                    throws SAXException {
//                if (qName.equalsIgnoreCase("student")) {
//                    String rollNo = attributes.getValue("rollno");
//                    System.out.println("Roll No : " + rollNo);
//                } else if (qName.equalsIgnoreCase("firstname")) {
//                    bFirstName = true;
//                } else if (qName.equalsIgnoreCase("lastname")) {
//                    bLastName = true;
//                } else if (qName.equalsIgnoreCase("nickname")) {
//                    bNickName = true;
//                }
//                else if (qName.equalsIgnoreCase("marks")) {
//                    bMarks = true;
//                }
//            }
//
//
//            public void endElement(String uri,
//                                   String localName, String qName) throws SAXException {
//                if (qName.equalsIgnoreCase("student")) {
//                    System.out.println("End Element :" + qName);
//                }
//            }
//
//            @Override
//            public void characters(char ch[],
//                                   int start, int length) throws SAXException {
//                if (bFirstName) {
//                    System.out.println("First Name: "
//                            + new String(ch, start, length));
//                    bFirstName = false;
//                } else if (bLastName) {
//                    System.out.println("Last Name: "
//                            + new String(ch, start, length));
//                    bLastName = false;
//                } else if (bNickName) {
//                    System.out.println("Nick Name: "
//                            + new String(ch, start, length));
//                    bNickName = false;
//                } else if (bMarks) {
//                    System.out.println("Marks: "
//                            + new String(ch, start, length));
//                    bMarks = false;
//                }
//            }
//        }

    }
    public static void main(String argv[]) {

//        readXMLDom();
//        readXmlSAX(inputFile);
    }

}