package se.kth.ik223x;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class RestJsonDataParser {

    /**
     * The url of a specific domain where patient vital signs are being read
     * and user information is available.
     */
    public static String URL = "<url>";

    /**
     * Obtains data from specific address and starts parsing json data.
     */
    public Serializable obtainData() {
        try {
            URL url = new URL(URL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);

            BufferedReader inputStream = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            StringBuilder jsonData = new StringBuilder();
            String str;
            while ((str = inputStream.readLine()) != null) {
                jsonData.append(str);
            }
            inputStream.close();

            return parseJsonData(jsonData.toString());
        } catch (MalformedURLException ex) {
            System.err.println("Provided URL seems incorrect.");
            ex.printStackTrace();
        } catch (IOException ex) {
            System.err.println("Connection problem has occurred during retrieving data.");
            ex.printStackTrace();
        }

        return null;
    }

    /**
     * Parses the json data obtained from specific address.
     *
     * @param jsonData
     * @return
     */
    private Serializable parseJsonData(String jsonData) {
        try {
            JSONObject json = new JSONObject(jsonData);
            String type = json.getString("type");

            if (type.equals("user")) {
                UserData user = new UserData();
                user.setFirstName(json.getString("firstname"));
                user.setLastName(json.getString("lastname"));
                user.setUsername(json.getString("username"));
                user.setPassword(json.getString("password"));// Security is not a concern of an ongoing prototype!
                user.setEmail(json.getString("email"));
                user.setRole(json.getString("role"));
                user.setCountry(json.getString("country"));
                user.setCounty(json.getString("county"));
                user.setCellPhone(json.getString("cell_phone"));
                user.setCity(json.getString("city"));
                user.setStreet(json.getString("street"));

                return user;
            } else if (type.equals("vital_signs")) {
                VitalSignData vitalSign = new VitalSignData();
                vitalSign.setUserId(json.getString("user_id"));
                vitalSign.setUsername(json.getString("username"));
                vitalSign.setPulse_rate(json.getInt("pulse_rate"));
                vitalSign.setBloodGlucose(json.getInt("blood_glucose"));
                vitalSign.setBodyPressure(json.getInt("blood_pressure"));
                vitalSign.setBodyTemperature((float) json.getDouble("body_temperature"));
                vitalSign.setEndTidalCo2((float) json.getDouble("end_tidal"));
                vitalSign.setForcedExpiratoryFlow(json.getInt("fef"));
                vitalSign.setForcedInspiratoryFlow(json.getInt("fif"));
                vitalSign.setGaitSpeed((float) json.getDouble("gait_speed"));
                vitalSign.setSpo2(json.getInt("spo2"));
                vitalSign.setVitalCapacity((float) json.getDouble("vital_capacity"));
                vitalSign.setTidalVolume(json.getInt("tidal_volume"));
                vitalSign.setRespirationRate((float) json.getDouble("respiration_rate"));

                return vitalSign;
            }
        } catch (JSONException ex) {
            System.err.println("JSON Exception was thrown during parsing JSON data.");
            ex.printStackTrace();
        }

        return null;
    }
}
