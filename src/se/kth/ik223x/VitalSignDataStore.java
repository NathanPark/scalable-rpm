package se.kth.ik223x;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class VitalSignDataStore {

    /**
     * Creates 'user' table both in Hive and HBase.
     */
    public void createHiveIntegratedVitalSignTable() {
        StringBuilder hql = new StringBuilder();// Row key in this table is going to be a 'username+timestamp';
        hql.append("CREATE TABLE IF NOT EXISTS vital_sign (id STRING, pulse_rate INT, spo2 INT, body_temperature FLOAT, ");
        hql.append("body_pressure INT, respiration_rate FLOAT, blood_glucose INT, vital_capacity FLOAT, ");
        hql.append("forced_expiratory_flow INT, forced_inspiratory_flow INT, tidal_volume INT, ");
        hql.append("end_tidal_co2 FLOAT, gait_speed FLOAT, user_id STRING, date TIMESTAMP)");
        hql.append("STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'");
        hql.append("WITH SERDEPROPERTIES('hbase.columns.mapping'=':key, records:val')");
        hql.append("TBLPROPERTIES('hbase.table.name'='vital_sign')");

        try {
            Statement statement = HiveSettings.getStatement();
            statement.executeQuery(hql.toString());
        } catch (SQLException ex) {
            System.err.println("\nError occurred during creating 'vital_sign' table.\n");
            ex.printStackTrace();
        }
    }

    /**
     * Returns medical records of a specific patient in last one week.
     *
     * @param patientId
     */
    public ArrayList<VitalSignData> getPatientRecordsForLastWeek(String patientId) {
        long today = System.currentTimeMillis();
        long weekAgo = today - 7 * 24 * 60 * 60 * 1000;
        StringBuilder hql = new StringBuilder();
        hql.append("SELECT * FROM vital_sign WHERE user_id=" + patientId + " AND date > " + weekAgo + " AND date <= " + today);

        try {
            Statement statement = HiveSettings.getStatement();
            ResultSet result = statement.executeQuery(hql.toString());

            ArrayList<VitalSignData> dataList = new ArrayList<VitalSignData>();
            while(result.next()) {
                VitalSignData data = new VitalSignData();
                data.setPulse_rate(result.getInt("pulse_rate"));
                data.setRespirationRate(result.getFloat("respiration_rate"));
                data.setSpo2(result.getInt("spo2"));
                data.setTidalVolume(result.getInt("tidal_volume"));
                data.setGaitSpeed(result.getFloat("gait_speed"));
                data.setVitalCapacity(result.getFloat("vital_capacity"));
                data.setBloodGlucose(result.getInt("blood_glucose"));
                data.setBodyTemperature(result.getFloat("blood_temperature"));
                data.setBodyPressure(result.getInt("body_pressure"));
                data.setEndTidalCo2(result.getFloat("end_tidal_co2"));
                data.setForcedExpiratoryFlow(result.getInt("forced_expiratory_flow"));
                data.setForcedInspiratoryFlow(result.getInt("forced_inspiratory_flow"));

                dataList.add(data);
            }

            return dataList;
        } catch (SQLException ex) {
            System.err.println("\nError occurred during creating 'vital_sign' table.\n");
            ex.printStackTrace();
        }

        return null;
    }
}
