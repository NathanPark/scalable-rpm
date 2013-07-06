package se.kth.ik223x;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Connection settings of Hive Server which stores all METADATA regarding
 * to Hive including tables and data statistics.
 */
public class HiveSettings {

    private static final String HIVE_SERVER_HOST = "jdbc:hive://localhost";
    private static final String HIVE_SERVER_PORT = "10000";
    private static final String HIVE_SERVER_URL = HIVE_SERVER_HOST + ":" + HIVE_SERVER_PORT + "/default";

    private static String HIVE_PASSWORD = "<passphrase>";
    private static String HIVE_USERNAME = "<username>";

    private static Connection connection;

    /**
     * Returns an existing connection, if none exists, new connection will be created
     * based on the server details and user credentials.
     *
     * @return - returns Hive Server connection
     */
    public static Connection getConnection() {
        if (connection == null) {
            try {
                String hiveDriver = "org.apache.hadoop.hive.jdbc.HiveDriver";
                Class.forName(hiveDriver);

                connection = DriverManager.getConnection(HIVE_SERVER_URL, HIVE_USERNAME, HIVE_PASSWORD);
            } catch (ClassNotFoundException ex) {
                System.err.println("\n***** Hive Driver was not found in the classpath. *****\n");
                ex.printStackTrace();
            } catch (SQLException ex) {
                System.err.println("\n***** Error occurred during connecting to Hive Server *****\n");
                ex.printStackTrace();
            }
            return connection;
        }

        return connection;
    }

    /**
     * Returns the statement to execute queries.
     *
     * @return - returns statements.
     * @throws SQLException
     */
    public static Statement getStatement() throws SQLException {
        return getConnection().createStatement();
    }
}
