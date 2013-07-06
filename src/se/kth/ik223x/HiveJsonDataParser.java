package se.kth.ik223x;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

public class HiveJsonDataParser {

    public static final String CITY_PATIENTS = "CITY_PATIENTS";
    public static final String LAST_WEEK_STATISTICS = "LAST_WEEK_STATISTICS";

    public static String URL = "<url>";

    private static HttpURLConnection connection;

    private HiveDataProvider<String, String> dataProvider;

    /**
     * Obtains JSON data and parses it to execute requests
     */
    public void obtainAndParse() {
        try {
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            StringBuilder jsonData = new StringBuilder();
            String str;
            while ((str = inputStream.readLine()) != null) {
                jsonData.append(str);
            }
            inputStream.close();

            JSONObject json = new JSONObject(jsonData);
            String type = json.getString("type");

            if (type.equals(CITY_PATIENTS)) {
                if (dataProvider != null) {
                    dataProvider.obtain(json.getString("city"), CITY_PATIENTS);
                }
            } else if (type.equals(LAST_WEEK_STATISTICS)) {
                if (dataProvider != null) {
                    dataProvider.obtain(json.getString("user_id"), LAST_WEEK_STATISTICS);
                }
            }
        } catch (MalformedURLException ex) {
            System.err.println("Provided URL seems incorrect.");
            ex.printStackTrace();
        } catch (IOException ex) {
            System.err.println("Connection problem has occurred during retrieving data.");
            ex.printStackTrace();
        } catch (JSONException ex) {
            System.err.println("JSON exception occurred during parsing JSON data.");
            ex.printStackTrace();
        }
    }

    /**
     * Sends patient list of a specific city to the specified address where
     * it is shown to clinicians.
     *
     * @param patients
     */
    public void sendPatientsListInCity(ArrayList<UserData> patients) {
        try {
            OutputStreamWriter writer = new OutputStreamWriter(getConnection().getOutputStream());

            for (UserData patient : patients) {
                StringBuilder builder = new StringBuilder();
                builder.append(patient.getFirstName());
                builder.append(patient.getLastName());
                builder.append(patient.getUsername());
                builder.append(patient.getEmail());
                builder.append(patient.getCountry());
                builder.append(patient.getCounty());
                builder.append(patient.getCity());
                builder.append(patient.getStreet());
                builder.append(patient.getCellPhone());

                writer.write(builder.toString());
            }

            writer.flush();
            writer.close();
        } catch (IOException ex){
            System.err.println("Error during sending patients list");
            ex.printStackTrace();
        }
    }

    /**
     * Sends patient records for last week to the specified address where it
     * is shown to clinicians
     *
     * @param vitalSignList
     */
    public void sendPatientRecordsForLastWeek(ArrayList<VitalSignData> vitalSignList) {
        try {
            OutputStreamWriter writer = new OutputStreamWriter(getConnection().getOutputStream());

            for (VitalSignData vitalSign : vitalSignList) {
                StringBuilder builder = new StringBuilder();
                builder.append(vitalSign.getBloodGlucose());
                builder.append(vitalSign.getBodyPressure());
                builder.append(vitalSign.getBodyTemperature());
                builder.append(vitalSign.getEndTidalCo2());
                builder.append(vitalSign.getForcedExpiratoryFlow());
                builder.append(vitalSign.getForcedInspiratoryFlow());
                builder.append(vitalSign.getGaitSpeed());
                builder.append(vitalSign.getPulse_rate());
                builder.append(vitalSign.getSpo2());
                builder.append(vitalSign.getRespirationRate());
                builder.append(vitalSign.getVitalCapacity());
                builder.append(vitalSign.getTidalVolume());
                builder.append(vitalSign.getUsername());

                writer.write(builder.toString());
            }

            writer.flush();
            writer.close();
        } catch (IOException ex){
            System.err.println("Error during sending patients list");
            ex.printStackTrace();
        }
    }

    /**
     * Sets data provider interface
     *
     * @param dataProvider
     */
    public void setDataProvider(HiveDataProvider<String, String> dataProvider) {
        this.dataProvider = dataProvider;
    }

    /**
     * Returns HTTP connection.
     *
     * @return
     * @throws IOException
     */
    private HttpURLConnection getConnection() throws IOException {
        if (connection == null) {
            URL url = new URL(URL);

            connection = (HttpURLConnection) url.openConnection();
            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);
        }

        return connection;
    }
}
