package se.kth.ik223x;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Random;

/**
 * Current class requires REST server to be ON in HBase.
 */
public class HBaseRest {

    private static final String USER_COLUMN_FAMILY = "data";
    private static final String USER_TABLE = "user";
    private static final String VITAL_SIGN_COLUMN_FAMILY = "records";
    private static final String VITAL_SIGN_TABLE = "vital_sign";

    public static String CLUSTER_HOST = "cluster_host";// The host of REST server
    public static int CLUSTER_PORT = 9999;// The port which REST server is listening.

    public HBaseRest() {
        try {
            Configuration config = HBaseConfiguration.create();
            HBaseAdmin admin = new HBaseAdmin(config);
            // creates a 'user' table if none exists
            if (!admin.tableExists(USER_TABLE)) {
                createUserTable(config);
            }
            // creates a 'vital_sign' table if none exists
            if (!admin.tableExists(VITAL_SIGN_TABLE)) {
                createVitalSignTable(config);
            }
        } catch (IOException ex) {
            System.err.println("\nError occurred during configuring HBase.\n");
            ex.printStackTrace();
        }
    }

    /**
     * Adds a row with data to 'user' table.
     *
     * @param client - the client that trying to connect REST server
     * @param userData - user related data is being written
     */
    public void addRowToUserTable(Client client, UserData userData) {
        Random random = new Random(System.currentTimeMillis());
        String id = System.nanoTime() + "_" + random.nextInt(99999);
        byte[] row = Bytes.toBytes(id);

        Put put = new Put(row);
        fillRow(put, "cell_phone", userData.getCellPhone());
        fillRow(put, "city", userData.getCity());
        fillRow(put, "country", userData.getCountry());
        fillRow(put, "county", userData.getCounty());
        fillRow(put, "email", userData.getEmail());
        fillRow(put, "first_name", userData.getFirstName());
        fillRow(put, "last_name", userData.getLastName());
        fillRow(put, "password", userData.getPassword());
        fillRow(put, "role", userData.getRole());
        fillRow(put, "street", userData.getStreet());
        fillRow(put, "username", userData.getUsername());

        try {
            RemoteHTable table = new RemoteHTable(client, USER_TABLE);
            table.put(put);
        } catch (IOException ex) {
            System.err.println("\nError occurred during adding row to 'user' table.\n");
            ex.printStackTrace();
        }
    }

    /**
     * Adds a row with data to 'vital_sign' table.
     *
     * @param client - the client that trying to connect REST server
     * @param vitalSigns - vital sign values of a specific patient related data is being written
     */
    public void addRowToVitalSignTable(Client client, VitalSignData vitalSigns) {
        byte[] row = Bytes.toBytes(vitalSigns.generateRowKey());

        Put put = new Put(row);
        fillRow(put, "uid", vitalSigns.generateRowKey());
        fillRow(put, "user_id", vitalSigns.getUserId());
        fillRow(put, "blood_glucose", vitalSigns.getBloodGlucose());
        fillRow(put, "body_pressure", vitalSigns.getBodyPressure());
        fillRow(put, "body_temperature", vitalSigns.getBodyTemperature());
        fillRow(put, "end_tidal_co2", vitalSigns.getEndTidalCo2());
        fillRow(put, "forced_expiratory_flow", vitalSigns.getForcedExpiratoryFlow());
        fillRow(put, "forced_inspiratory_flow", vitalSigns.getForcedInspiratoryFlow());
        fillRow(put, "gait_speed", vitalSigns.getGaitSpeed());
        fillRow(put, "pulse_rate", vitalSigns.getPulse_rate());
        fillRow(put, "respiration_rate", vitalSigns.getRespirationRate());
        fillRow(put, "spo2", vitalSigns.getSpo2());
        fillRow(put, "tidal_volume", vitalSigns.getTidalVolume());
        fillRow(put, "vital_capacity", vitalSigns.getVitalCapacity());

        try {
            RemoteHTable table = new RemoteHTable(client, VITAL_SIGN_TABLE);
            table.put(put);
        } catch (IOException ex) {
            System.err.println("\nError occurred during adding row to 'vital_sign' table.\n");
            ex.printStackTrace();
        }
    }

    /**
     * Creates a 'user' table is it does not exist.
     *
     * @param config - HBase configuration
     */
    public void createUserTable(Configuration config) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor tableDescriptor = new HTableDescriptor(USER_TABLE);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(USER_COLUMN_FAMILY);
            tableDescriptor.addFamily(columnDescriptor);
            admin.createTable(tableDescriptor);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Creates a 'vital_sign' table is it does not exist.
     *
     * @param config - HBase configuration
     */
    public void createVitalSignTable(Configuration config) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor tableDescriptor = new HTableDescriptor(VITAL_SIGN_TABLE);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(VITAL_SIGN_COLUMN_FAMILY);
            tableDescriptor.addFamily(columnDescriptor);
            admin.createTable(tableDescriptor);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Puts a float value to the row of HBase table.
     *
     * @param put
     * @param qualifierString
     * @param vitalSign
     */
    private void fillRow(Put put, String qualifierString, float vitalSign) {
        byte[] columnFamily = Bytes.toBytes(VITAL_SIGN_COLUMN_FAMILY);
        byte[] qualifier = Bytes.toBytes(qualifierString);
        byte[] value = Bytes.toBytes(vitalSign);

        put.add(columnFamily, qualifier, value);
    }

    /**
     * Puts an integer value to the row of HBase table.
     *
     * @param put
     * @param qualifierString
     * @param vitalSign
     */
    private void fillRow(Put put, String qualifierString, int vitalSign) {
        byte[] columnFamily = Bytes.toBytes(VITAL_SIGN_COLUMN_FAMILY);
        byte[] qualifier = Bytes.toBytes(qualifierString);
        byte[] value = Bytes.toBytes(vitalSign);

        put.add(columnFamily, qualifier, value);
    }

    /**
     * Puts a string value to the row of HBase table
     *
     * @param put
     * @param qualifierString
     * @param userData
     */
    private void fillRow(Put put, String qualifierString, String userData) {
        byte[] columnFamily = Bytes.toBytes(USER_COLUMN_FAMILY);
        byte[] qualifier = Bytes.toBytes(qualifierString);
        byte[] value = Bytes.toBytes(userData);

        put.add(columnFamily, qualifier, value);
    }
}
