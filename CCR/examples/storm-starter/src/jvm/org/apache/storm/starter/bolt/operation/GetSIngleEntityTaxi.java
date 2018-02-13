package org.apache.storm.starter.bolt.operation;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;

import java.io.ByteArrayOutputStream;
import java.util.Random;

/**
 * Created by anshushukla on 27/04/16.
 */
public class GetSIngleEntityTaxi {

    public static final String storageConnectionString ="DefaultEndpointsProtocol=https;AccountName=ravikanttes;AccountKey=4mZBlhIyQ8vu3XAl6Zj69poK8BLF7b4avAnVSclTOcbKOsBaDLo/hAvRWTl+Yk1nBVpP9ftd2rr+P9hT0IrltA==";
//            "DefaultEndpointsProtocol=https;AccountName=ravikanttes;AccountKey=2KVRD/Htxsusi9Mtr9i1buBwz27k2oH+Mt28wFDBBjQuo4fZvapqbhh8szORKv4EpWyqJC0SdJ1WSiuaCK/ylA==";

    public static void main(String[] args) {

        CloudTable cloudTable = GetSIngleEntityTaxi.createAzureTableCon();

        for (int i = 0; i < 5000; i++) {
            Random r=new Random();
            long ts1 = System.nanoTime(); //addon
            String en=""+r.nextInt(202500);
//            String en="12";
            GetSIngleEntityTaxi.doAzureTableOp(cloudTable,en);
//            System.out.println("Length -"+en+"-"+GetSIngleEntityTaxi.doAzureTableOp(cloudTable,en));
            long ts2 = System.nanoTime(); //addon
            System.out.println("CHECK1 in Milli.Sec:-" + String.valueOf((ts2 - ts1) / Math.pow(10, 6)));
        }
    }

    public  static CloudTable createAzureTableCon() {
        CloudTable cloudTable = null;
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            // Create the table client
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
//                CloudTable cloudTable = tableClient.getTableReference("par2sample5");
            cloudTable = tableClient.getTableReference("partition1");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cloudTable;
    }

    public  static int doAzureTableOp(CloudTable cloudTable,String _rowkey){
        try{

            // Retrieve the entity with partition key of "Smith" and row key of "Jeff"
            TableOperation retrieveSmithJeff =
                    TableOperation.retrieve("1", _rowkey, CustomerEntityTaxi.class);

            // Submit the operation to the table service and get the specific entity.
            CustomerEntityTaxi specificEntity =
                    cloudTable.execute(retrieveSmithJeff).getResultAsType();

            // Output the entity.
            if (specificEntity != null) {

//                System.out.println(specificEntity.getPartitionKey() +
//                        "\t " + specificEntity.getRowKey()+"\t"+specificEntity.getDropoff_longitude());

                return specificEntity.getDropoff_longitude().length();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }


    public static CloudBlob connectToBlob()
    {
        long ts1=0;
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.getContainerReference("mycontainer");

            ListBlobItem blobItem = container.getAppendBlobReference("myimage.jpg");
            CloudBlob blob = (CloudBlob) blobItem;

            return blob;
        }
        catch (Exception e)
        {
            // Output the stack trace.
            e.printStackTrace();
        }


        return null;
    }

    public static int downloadBlobFromContainer(CloudBlob blob)
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream(1300);
        try {

//            //uncomment below line for testing
//                            blob.download(new FileOutputStream("/Users/anshushukla/Desktop/" + blob.getName()));

                blob.download(output);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        return output.size();
    }

}
