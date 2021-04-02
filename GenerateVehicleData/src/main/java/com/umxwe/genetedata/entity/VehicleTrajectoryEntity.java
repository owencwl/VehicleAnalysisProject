package com.umxwe.genetedata.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @ClassName VehicleEntity
 * @Description 车辆轨迹数据实体
 * @Author owen(umxwe)
 * @Date 2021/2/5
 */
public class VehicleTrajectoryEntity {

//    private int Calling;
//    private String Direction;
//    private String HasPlate;
//    private int InfoKind;
//    private int LaneNo;
//    private int LeftTopX;
//    private int LeftTopY;
//    private int RightBtmX;
//    private int RightBtmY;
//    private String MotorVehicleID;
//    private String PlateClass;
//    private String PlateColor;
//    private String PlateNo;
//    private int SafetyBelt;
//    private String SourceID;
//    private String StorageUrl1;
//    private String StorageUrl2;
//    private String StorageUrl3;
//    private int Sunvisor;
//    private String VehicleBrand;
//    private String VehicleClass;
//    private String VehicleColor;
//    private int VehicleLength;
//    private int initStatus;
//    private String plateClassDesc;
//    private String plateColorDesc;
//    private String storagePath;
//    private String subStoragePath;
//    private String vehicleBrandDesc;
//    private String vehicleClassDesc;
//    private String vehicleColorDesc;
//    private String MarkTime;//位置标记时间 人工采集时有效
//    private String PassTime;//车辆经过时间，卡口过车时间


    private String PlateClass;//车牌种类
    private String PlateColor;//车牌颜色
    private String plateClassDesc;//车牌种类描述
    private String plateColorDesc;//车牌颜色描述

    private String PlateNo;//车牌号码
    private String VehicleBrand;//车辆品牌
    private String VehicleClass;//车辆类别
    private String VehicleColor;//车辆颜色
    private String vehicleBrandDesc;//车辆品牌描述
    private String vehicleClassDesc;//车辆种类描述
    private String vehicleColorDesc;//车辆颜色描述


    private String DeviceID;//抓拍设备id
    private String address;//抓拍设备地址
    private String deviceName;//抓拍设备名称
    private long shotTime;//抓拍时间
    private String rowKey;
    private double shotPlaceLatitude;//抓拍地点纬度
    private double shotPlaceLongitude;//抓拍地点经度
    private String location;//"维度,经度"字符串，用于es中

    private double speed;

    public Map<String, Object> toMap() {

        Map<String, Object> vehiclemap = new HashMap<>();
        vehiclemap.put("PlateClass", this.getPlateClass());
        vehiclemap.put("PlateColor", this.getPlateColor());
        vehiclemap.put("plateClassDesc", this.getPlateClassDesc());
        vehiclemap.put("plateColorDesc", this.getPlateColorDesc());
        vehiclemap.put("PlateNo", this.getPlateNo());
        vehiclemap.put("VehicleBrand", this.getVehicleBrand());
        vehiclemap.put("VehicleClass", this.getVehicleClass());
        vehiclemap.put("VehicleColor", this.getVehicleColor());
        vehiclemap.put("vehicleBrandDesc", this.getVehicleBrandDesc());
        vehiclemap.put("vehicleClassDesc", this.getVehicleClassDesc());
        vehiclemap.put("vehicleColorDesc", this.getVehicleColorDesc());
        vehiclemap.put("DeviceID", this.getDeviceID());
        vehiclemap.put("address", this.getAddress());
        vehiclemap.put("deviceName", this.getDeviceName());
        vehiclemap.put("shotTime", this.getShotTime());
        vehiclemap.put("rowKey", this.getRowKey());
        vehiclemap.put("shotPlaceLatitude", this.getShotPlaceLatitude());
        vehiclemap.put("shotPlaceLongitude", this.getShotPlaceLongitude());
        vehiclemap.put("location", this.getShotPlaceLatitude() + "," + this.getShotPlaceLongitude());
        return vehiclemap;
    }


    public VehicleTrajectoryEntity() {
        Integer deviceId = new Random().nextInt(100);
        this.setDeviceID("43120000001190000" + deviceId);
        this.setAddress("湖南省怀化市溆浦县卢峰镇" + deviceId);
        this.setDeviceName("摄像头设备" + deviceId);

        /**
         * 获取一个月范围的随机时间
         */
        long rangebegin = Timestamp.valueOf("2021-01-01 00:00:00").getTime();
        long rangeend = Timestamp.valueOf("2021-01-31 00:59:59").getTime();
        long diff = rangeend - rangebegin + 1;
        Timestamp rand = new Timestamp(rangebegin + (long) (Math.random() * diff));
        rand.getTime();
//        this.setShotTime(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(rand.toLocalDateTime()));
        this.setShotTime(rand.getTime());
        double lat = randomLonLat("Lat");
        double lon = randomLonLat("Lon");

        this.setShotPlaceLatitude(lat);
        this.setShotPlaceLongitude(lon);
        this.setLocation(lat + "," + lon);

    }

    /**
     * @return
     * @throws
     * @Title: randomLonLat
     * @Description: 在矩形内随机生成经纬度 MinLon：最小经度  MaxLon： 最大经度   MinLat：最小纬度   MaxLat：最大纬度
     * type：设置返回经度还是纬度
     */
    public double randomLonLat(String type) {
        //默认长沙区域的经纬度
        //112.821726,28.323551 望城西北角
        //113.182198,28.063764 高铁南站东南角
        double MinLon = 112.821726;
        double MaxLon = 113.182198;
        double MinLat = 28.063764;
        double MaxLat = 28.323551;
        BigDecimal db = new BigDecimal(Math.random() * (MaxLon - MinLon) + MinLon);
        double lon = db.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();// 小数后6位
        db = new BigDecimal(Math.random() * (MaxLat - MinLat) + MinLat);
        double lat = db.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
        if (type.equals("Lon")) {
            return lon;
        } else {
            return lat;
        }
    }

    public String getDeviceID() {
        return DeviceID;
    }

    public void setDeviceID(String deviceID) {
        DeviceID = deviceID;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    public long getShotTime() {
        return shotTime;
    }

    public void setShotTime(long shotTime) {
        this.shotTime = shotTime;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public double getShotPlaceLatitude() {
        return shotPlaceLatitude;
    }

    public void setShotPlaceLatitude(double shotPlaceLatitude) {
        this.shotPlaceLatitude = shotPlaceLatitude;
    }

    public double getShotPlaceLongitude() {
        return shotPlaceLongitude;
    }

    public void setShotPlaceLongitude(double shotPlaceLongitude) {
        this.shotPlaceLongitude = shotPlaceLongitude;
    }

    public String getPlateClass() {
        return PlateClass;
    }

    public void setPlateClass(String plateClass) {
        PlateClass = plateClass;
    }

    public String getPlateColor() {
        return PlateColor;
    }

    public void setPlateColor(String plateColor) {
        PlateColor = plateColor;
    }

    public String getPlateClassDesc() {
        return plateClassDesc;
    }

    public void setPlateClassDesc(String plateClassDesc) {
        this.plateClassDesc = plateClassDesc;
    }

    public String getPlateColorDesc() {
        return plateColorDesc;
    }

    public void setPlateColorDesc(String plateColorDesc) {
        this.plateColorDesc = plateColorDesc;
    }

    public String getPlateNo() {
        return PlateNo;
    }

    public void setPlateNo(String plateNo) {
        PlateNo = plateNo;
    }

    public String getVehicleBrand() {
        return VehicleBrand;
    }

    public void setVehicleBrand(String vehicleBrand) {
        VehicleBrand = vehicleBrand;
    }

    public String getVehicleClass() {
        return VehicleClass;
    }

    public void setVehicleClass(String vehicleClass) {
        VehicleClass = vehicleClass;
    }

    public String getVehicleColor() {
        return VehicleColor;
    }

    public void setVehicleColor(String vehicleColor) {
        VehicleColor = vehicleColor;
    }

    public String getVehicleBrandDesc() {
        return vehicleBrandDesc;
    }

    public void setVehicleBrandDesc(String vehicleBrandDesc) {
        this.vehicleBrandDesc = vehicleBrandDesc;
    }

    public String getVehicleClassDesc() {
        return vehicleClassDesc;
    }

    public void setVehicleClassDesc(String vehicleClassDesc) {
        this.vehicleClassDesc = vehicleClassDesc;
    }

    public String getVehicleColorDesc() {
        return vehicleColorDesc;
    }

    public void setVehicleColorDesc(String vehicleColorDesc) {
        this.vehicleColorDesc = vehicleColorDesc;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }
}