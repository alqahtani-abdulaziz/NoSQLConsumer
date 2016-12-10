import com.microsoft.azure.storage.table.TableServiceEntity;

public class VehicleEntity extends TableServiceEntity {

    public VehicleEntity(String registration,String isOffender){
        this.rowKey = registration;
        this.partitionKey = isOffender;
    }

    public  VehicleEntity(){}

    private String registration;
    private String type;
    private int speed;
    private boolean isOffender;
    private String cameraID;

    public void setOffender(boolean offender) {
        isOffender = offender;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public void setCameraID(String cameraID) {
        this.cameraID = cameraID;
    }

    public void setRegistration(String registration) {
        this.registration = registration;
    }

    public String getType() {
        return type;
    }

    public String getRegistration() {
        return registration;
    }

    public int getSpeed() {
        return speed;
    }

    public String getCameraID() {
        return cameraID;
    }

    public boolean isOffender() {
        return isOffender;
    }
}
