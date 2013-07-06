package se.kth.ik223x;

public class HBaseHive {

    private UserDataStore userData = new UserDataStore();
    private VitalSignDataStore vitalSign = new VitalSignDataStore();

    public HBaseHive() {
        userData.createHiveIntegratedUserTable();
        vitalSign.createHiveIntegratedVitalSignTable();
    }

    /**
     * Returns 'user' table
     *
     * @return
     */
    public UserDataStore getUserData() {
        return userData;
    }

    /**
     * Returns 'vital_sign' table
     *
     * @return
     */
    public VitalSignDataStore getVitalSign() {
        return vitalSign;
    }
}
