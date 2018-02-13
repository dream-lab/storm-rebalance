package org.apache.storm.starter.bolt.operation;

import com.microsoft.azure.storage.table.TableServiceEntity;


public class CustomerEntityTaxi extends TableServiceEntity {
//    public CustomerEntityTaxi(String lastName, String firstName) {
//        this.partitionKey = lastName;
//        this.rowKey = firstName;
//    }

    public CustomerEntityTaxi() { }

    String Dropoff_datetime;
    String Dropoff_longitude;

    public String getDropoff_datetime() {
        return this.Dropoff_datetime;
    }

    public String getDropoff_longitude() {
        return this.Dropoff_longitude;
    }


    public void setDropoff_datetime(String Dropoff_datetime) {
        this.Dropoff_datetime = Dropoff_datetime;
    }

//    public String getPhoneNumber() {
//        return this.phoneNumber;
//    }

    public void setDropoff_longitude(String Dropoff_longitude) {
        this.Dropoff_longitude = Dropoff_longitude;
    }

}