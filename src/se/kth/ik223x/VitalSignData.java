package se.kth.ik223x;

import java.io.Serializable;

public class VitalSignData implements Serializable {

    private int bloodGlucose;
    private int bodyPressure;
    private float bodyTemperature;
    private float endTidalCo2;
    private int forcedExpiratoryFlow;
    private int forcedInspiratoryFlow;
    private float gaitSpeed;
    private int pulse_rate;
    private float respirationRate;
    private int spo2;
    private int tidalVolume;
    private String userId;
    private String username;
    private float vitalCapacity;

    public String generateRowKey() {
        StringBuilder rowKey = new StringBuilder();
        rowKey.append(username).append("+");
        rowKey.append(System.currentTimeMillis());

        return rowKey.toString();
    }

    public int getBloodGlucose() {
        return bloodGlucose;
    }

    public void setBloodGlucose(int bloodGlucose) {
        this.bloodGlucose = bloodGlucose;
    }

    public int getBodyPressure() {
        return bodyPressure;
    }

    public void setBodyPressure(int bodyPressure) {
        this.bodyPressure = bodyPressure;
    }

    public float getBodyTemperature() {
        return bodyTemperature;
    }

    public void setBodyTemperature(float bodyTemperature) {
        this.bodyTemperature = bodyTemperature;
    }

    public float getEndTidalCo2() {
        return endTidalCo2;
    }

    public void setEndTidalCo2(float endTidalCo2) {
        this.endTidalCo2 = endTidalCo2;
    }

    public int getForcedExpiratoryFlow() {
        return forcedExpiratoryFlow;
    }

    public void setForcedExpiratoryFlow(int forcedExpiratoryFlow) {
        this.forcedExpiratoryFlow = forcedExpiratoryFlow;
    }

    public int getForcedInspiratoryFlow() {
        return forcedInspiratoryFlow;
    }

    public void setForcedInspiratoryFlow(int forcedInspiratoryFlow) {
        this.forcedInspiratoryFlow = forcedInspiratoryFlow;
    }

    public float getGaitSpeed() {
        return gaitSpeed;
    }

    public void setGaitSpeed(float gaitSpeed) {
        this.gaitSpeed = gaitSpeed;
    }

    public int getPulse_rate() {
        return pulse_rate;
    }

    public void setPulse_rate(int pulse_rate) {
        this.pulse_rate = pulse_rate;
    }

    public float getRespirationRate() {
        return respirationRate;
    }

    public void setRespirationRate(float respirationRate) {
        this.respirationRate = respirationRate;
    }

    public int getSpo2() {
        return spo2;
    }

    public void setSpo2(int spo2) {
        this.spo2 = spo2;
    }

    public int getTidalVolume() {
        return tidalVolume;
    }

    public void setTidalVolume(int tidalVolume) {
        this.tidalVolume = tidalVolume;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public float getVitalCapacity() {
        return vitalCapacity;
    }

    public void setVitalCapacity(float vitalCapacity) {
        this.vitalCapacity = vitalCapacity;
    }
}
