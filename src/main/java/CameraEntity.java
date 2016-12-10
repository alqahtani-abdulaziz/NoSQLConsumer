import com.microsoft.azure.storage.table.TableServiceEntity;

public class CameraEntity extends TableServiceEntity {

    public CameraEntity(String uid,String city){
        this.rowKey = uid;
        this.partitionKey = city;

    }
    public CameraEntity(){}

    private String uid;
    private String streetName;
    private String city;
    private int speedLimit;

    public void setCity(String city) {
        this.city = city;
    }

    public void setSpeedLimit(int speedLimit) {
        this.speedLimit = speedLimit;
    }

    public void setStreetName(String streetName) {
        this.streetName = streetName;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getSpeedLimit() {
        return speedLimit;
    }

    public String getCity() {
        return city;
    }

    public String getStreetName() {
        return streetName;
    }

    public String getUid() {
        return uid;
    }
}
