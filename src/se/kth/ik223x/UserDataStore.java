package se.kth.ik223x;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class UserDataStore {

    /**
     * Creates 'user' table both in Hive and HBase.
     * As security is not considered in current phase,
     * all passwords will not be hashed for now.
     */
    public void createHiveIntegratedUserTable() {
        StringBuilder hql = new StringBuilder();
        hql.append("CREATE TABLE IF NOT EXISTS user (username STRING, first_name STRING, last_name STRING, ");
        hql.append("password STRING, role STRING, email STRING, cell_phone STRING, ");
        hql.append("country STRING, county STRING, city STRING, street STRING) ");
        hql.append("STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'");
        hql.append("WITH SERDEPROPERTIES('hbase.columns.mapping'=':key, person:val')");
        hql.append("TBLPROPERTIES('hbase.table.name'='user')");

        try {
            Statement statement = HiveSettings.getStatement();
            statement.executeQuery(hql.toString());
        } catch (SQLException ex) {
            System.err.println("\nError occurred during creating 'user' table.\n");
            ex.printStackTrace();
        }
    }

    public ArrayList<UserData> getAllPatientsByCity(String city) {
        StringBuilder hql = new StringBuilder();
        hql.append("SELECT * FROM user WHERE city=" +city);

        try {
            Statement statement = HiveSettings.getStatement();
            ResultSet result = statement.executeQuery(hql.toString());

            ArrayList<UserData> dataList = new ArrayList<UserData>();
            while(result.next()) {
                UserData data = new UserData();
                data.setFirstName(result.getString("first_name"));
                data.setLastName(result.getString("last_name"));
                data.setEmail(result.getString("email"));
                data.setStreet(result.getString("street"));
                data.setCellPhone(result.getString("cell_phone"));

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
