import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.*;
import com.microsoft.windowsazure.services.servicebus.models.*;
// Include the following imports to use table APIs
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.table.*;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;



public class Consumer {

    private static ServiceBusContract service;
    private static final String CAMERA_TABLE_NAME = "cameras";
    private static final String VEHICLE_TABLE_NAME = "vehicles";

    // Define the connection-string with your values.
    public static final String storageConnectionString =
            "DefaultEndpointsProtocol=http;" +
                    "AccountName=nosql;" +
                    "AccountKey=2d7s25YGjHn5GxcW0qW1DYsyMwcp0cxT9/z0jGpvoDZJTa9Q2R0pP7hizwEN9jSzoAil5KzrRzH8RTAQEP4rpw==";
    static Configuration config;
//    static String storageConnectionString;
    public static void main(String[] args){
        config = ServiceBusConfiguration.configureWithSASAuthentication(
                "smarcamera",
                "RootManageSharedAccessKey",
                "h9zP+sPjaennM/CI3rHJzoy+ymsSGJkcJM0/csNp7Vw=",
                ".servicebus.windows.net"

        );

        //Create a connection to the service
        service = ServiceBusService.create(config);
        prepareTables();
        readCameraMessages();
        readVehicleMessages();



    }

    public static void prepareTables(){

            try
            {
                // Retrieve storage account from connection-string.
                CloudStorageAccount storageAccount =
                        CloudStorageAccount.parse(storageConnectionString);

                // Create the table client.
                CloudTableClient tableClient = storageAccount.createCloudTableClient();

                // Create the table if it doesn't exist.
                CloudTable vehiclesTable = tableClient.getTableReference(VEHICLE_TABLE_NAME);
                CloudTable camerasTable = tableClient.getTableReference(CAMERA_TABLE_NAME);
                vehiclesTable.createIfNotExists();
                camerasTable.createIfNotExists();
            }
            catch (Exception e)
            {
                // Output the stack trace.
                e.printStackTrace();
            }

    }

    public static Iterable<String> getTables () {
        try
        {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            return tableClient.listTables();
        }
        catch (Exception e)
        {
            // Output the stack trace.
            e.printStackTrace();
            return null;
        }
    }
    public static void readCameraMessages(){
        try
        {
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
//
//            SubscriptionInfo subInfo = new SubscriptionInfo("AllMessages");
//            CreateSubscriptionResult result =
//                    service.createSubscription("cameramsgs", subInfo);
            while(true)  {
                ReceiveSubscriptionMessageResult resultQM =
                        service.receiveSubscriptionMessage("cameramsgs", "AllMessages");
                BrokeredMessage message = resultQM.getValue();
                if (message != null && message.getMessageId() != null)
                {
                    System.out.println("MessageID: " + message.getMessageId());
                    // Display the queue message.
                    System.out.print("From queue: ");
                    byte[] b = new byte[600];
                    String s = null;
                    int numRead = message.getBody().read(b);
                    ArrayList<String> output = new ArrayList<String>();
                    while (-1 != numRead)
                    {
                        s = new String(b);
                        s = s.trim();

                        System.out.print(s);


                        output.add(s);

                        numRead = message.getBody().read(b);

                    }



                    System.out.println();

                    String my = new String();
                    for(String x : output){
                        my += x;
                    }

                    writeCameraToTable(my);

                    System.out.println("Custom Property: " +
                            message.getProperty("MyProperty"));
                    // Remove message from queue.
                    System.out.println("Deleting this message.");
                    service.deleteMessage(message);
                }
                else
                {
                    System.out.println("Finishing up - no more messages.");
                    break;
                    // Added to handle no more messages.
                    // Could instead wait for more messages to be added.
                }
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());

        }
        catch (Exception e) {
            System.out.print("Generic exception encountered: ");
            System.out.println(e.getMessage());

        }
    }
    public static void readVehicleMessages(){
        try
        {
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            opts.setReceiveMode(ReceiveMode.PEEK_LOCK);

//            SubscriptionInfo subInfo = new SubscriptionInfo("AllMessages");
//            CreateSubscriptionResult result =
//                    service.createSubscription("vehiclemsgs", subInfo);

            while(true)  {
                ReceiveSubscriptionMessageResult resultQM =
                        service.receiveSubscriptionMessage("vehiclemsgs", "AllMessages");
                BrokeredMessage message = resultQM.getValue();
                if (message != null && message.getMessageId() != null)
                {
                    System.out.println("MessageID: " + message.getMessageId());
                    // Display the queue message.
                    System.out.print("From queue: ");
                    byte[] b = new byte[200];
                    String s = null;
                    int numRead = message.getBody().read(b);
                    ArrayList<String> output = new ArrayList<String>();
                    while (-1 != numRead)
                    {
                        s = new String(b);
                        s = s.trim();
                        output.add(s);


                        System.out.print(s);
                        numRead = message.getBody().read(b);
                    }

                    String my = new String();
                    for(String x : output){
                        my += x;
                    }


                    writeVehilceToTable(my);

                    System.out.println();
                    System.out.println("Custom Property: " +
                            message.getProperty("MyProperty"));
                    // Remove message from queue.
                    System.out.println("Deleting this message.");
                    service.deleteMessage(message);
                }
                else
                {
                    System.out.println("Finishing up - no more messages.");
                    break;
                    // Added to handle no more messages.
                    // Could instead wait for more messages to be added.
                }
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());

        }
        catch (Exception e) {
            System.out.print("Generic exception encountered: ");
            System.out.println(e.getMessage());

        }
    }

    public static void writeVehilceToTable(String message) throws JSONException, UnsupportedEncodingException {


        Gson gson = new Gson();
        JsonObject object = new JsonParser().parse(message).getAsJsonObject();




        VehicleEntity entity = new VehicleEntity(object.get("registration").getAsString(),object.get("isOffender").getAsString());
        entity.setRegistration(object.get("registration").getAsString());
        entity.setType(object.get("type").getAsString());
        entity.setSpeed(object.get("speed").getAsInt());
        entity.setCameraID(object.get("camera").getAsString());
        entity.setOffender(object.get("isOffender").getAsBoolean());



        try
        {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("vehicles");



            // Create an operation to add the new customer to the people table.
            TableOperation insertCustomer1 = TableOperation.insertOrReplace(entity);

            // Submit the operation to the table service.
            cloudTable.execute(insertCustomer1);
        }
        catch (Exception e)
        {
            // Output the stack trace.
            e.printStackTrace();
        }

    }

    public static void writeCameraToTable(String message) throws JSONException, IOException {

        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new StringReader(message));
        JsonObject object = new JsonParser().parse(reader).getAsJsonObject();

        CameraEntity entity = new CameraEntity(object.get("uid").getAsString(),object.get("city").getAsString());
        entity.setStreetName(object.get("streetName").getAsString());
        entity.setCity(object.get("city").getAsString());
        entity.setUid(object.get("uid").getAsString());
        entity.setSpeedLimit(object.get("speedLimit").getAsInt());

        try
        {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference("cameras");



            // Create an operation to add the new customer to the people table.
            TableOperation insertCustomer1 = TableOperation.insertOrReplace(entity);

            // Submit the operation to the table service.
            cloudTable.execute(insertCustomer1);
        }
        catch (Exception e)
        {
            // Output the stack trace.
            e.printStackTrace();
        }

    }

}
