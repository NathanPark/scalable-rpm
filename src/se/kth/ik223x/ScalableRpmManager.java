package se.kth.ik223x;

import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.Scanner;

public class ScalableRpmManager {

    public static final String HIVE_HBASE_APP = "HIVE_HBASE_APP";
    public static final String REST_HBASE_APP = "REST_HBASE_APP";

    public static void main(String[] args) throws IOException {
        mainWindow();
    }

    /**
     *  Scalable Application is divided into two parts where
     *  the first part is Apache Hive plus HBase and the second
     *  part is Apache HBase plus REST web services. Apache Hive integration
     *  part of the application is mostly oriented for
     *  different kinds of data statistics where REST part of the application
     *  is for direct communication with HBase data store through REST server.
     */
    private static void mainWindow() {
        Scanner input = new Scanner(System.in);

        System.out.println("1. Apache Hive and HBase oriented RPM");
        System.out.println("2. REST web-services and HBase oriented RPM");
        System.out.print("Please select the number from one of the options above:");

        try {
            int option = input.nextInt();
            if (option == 1) {
                hiveApp();
            } else if (option == 2) {
                restApp();
            } else {
                System.err.println("Please enter either 1 or 2.");
                mainWindow();
            }
        } catch (NumberFormatException ex) {
            ex.printStackTrace();
            System.err.println("Please enter either 1 or 2.");

            mainWindow();
        }
    }

    /**
     * Apache Hive plus HBase part of the application
     */
    private static void hiveApp() {
        final HBaseHive hive = new HBaseHive();

        final HiveJsonDataParser parser = new HiveJsonDataParser();
        parser.setDataProvider(new HiveDataProvider<String, String>() {
            @Override
            public void obtain(String key, String data) {
                if (data.equals(HiveJsonDataParser.CITY_PATIENTS)) {
                    parser.sendPatientsListInCity(hive.getUserData().getAllPatientsByCity(key));
                } else if (data.equals(HiveJsonDataParser.LAST_WEEK_STATISTICS)) {
                    parser.sendPatientRecordsForLastWeek(hive.getVitalSign().getPatientRecordsForLastWeek(key));
                }
            }
        });
    }

    /**
     *  Apache HBase plus REST part of the application
     */
    private static void restApp() {
        Cluster cluster = new Cluster();
        cluster.add(HBaseRest.CLUSTER_HOST, HBaseRest.CLUSTER_PORT);
        Client client = new Client(cluster);

        HBaseRest rest = new HBaseRest();

        RestJsonDataParser parser = new RestJsonDataParser();
        Serializable serializable = parser.obtainData();
        if (serializable instanceof UserData) {
            rest.addRowToUserTable(client, (UserData) serializable);
        } else if (serializable instanceof VitalSignData) {
            rest.addRowToVitalSignTable(client, (VitalSignData) serializable);
        }
    }
}
