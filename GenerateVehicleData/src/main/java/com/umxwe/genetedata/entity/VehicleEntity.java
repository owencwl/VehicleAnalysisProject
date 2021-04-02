package com.umxwe.genetedata.entity;

import com.umxwe.genetedata.utils.RandomDataUtil;

/**
 * @ClassName VehicleEntity
 * @Description 车辆基本信息数据
 * @Author owen(umxwe)
 * @Date 2021/2/5
 */
public class VehicleEntity {
    private String rowkey;
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

    public VehicleEntity() {
        this.setPlateClass(RandomDataUtil.randomValue("Civil license plate", "armed police license plate", "military vehicle license plate", "foreign license plate", "Trailer license plate", "embassy license plate", "Hong Kong Macao Entry exit license plate", "coach license plate"));
        this.setPlateClassDesc(RandomDataUtil.randomValue("民用车牌", "武警车牌", "军车车牌", "国外车牌", "挂车号牌", "使领馆牌", "港澳入出境牌", "教练车牌"));

        this.setPlateColor(RandomDataUtil.randomValue("Red", "black", "white", "green", "yellow", "gray", "Brown"));
        this.setPlateColorDesc(RandomDataUtil.randomValue("红", "黑 ", "白", "绿", "黄", "灰", "棕"));

        this.setPlateNo(((int) Math.random() * 100) % 100 == 0 ? RandomDataUtil.generateCarID()
                : RandomDataUtil.randomValue("湘A1NS20", "湘A2NN30", "湘A2NSV0", "湘A3NST0", "湘A4NS50", "湘ATNS60", "湘A4NS80"));

//        this.setPlateNo(RandomDataUtil.randomValue("湘A1NS20", "湘A2NN30", "湘A2NSV0", "湘A3NST0", "湘A4NS50", "湘ATNS60", "湘A4NS80"));

        this.setVehicleBrand(RandomDataUtil.randomValue("Red", "black", "white", "green", "yellow", "grey", "Brown", "Audi", "AC Schnitzer", "Artega", "Benz", "BMW", "Porsche", "babos", "Bovo", "VW",
                "KTM", "Carlson Bai", "mini", "Opel", "smart", "StarTech", "taikat", "weizman", "siyate", " Honda ", " Toyota ", " Guanggang ", " Suzuki ", " Lexus ",
                "Langshi", "Mazda", "Acura", "Nissan", "Mitsubishi", "Subaru", "Isuzu", "Infiniti", "Kia", "Shuanglong", "Hyundai", "American brands", "Buick", "dodge", "Ford", "GMC",
                "Jeep", "Cadillac", "Chrysler", "Lincoln", "George Barton", "Sam", "Tesla", "Chevrolet", "Hackett", "Eurosystem others", "Peugeot", "DS", "Renault", "PGO", "Citroen,",
                "Aston Martin", "Bentley", "Jaguar", "Land Rover", "Rolls Royce", "lutes", "McLaren", "Morgan", "noble", "Alfa Romeo", "Bugatti", "Ferrari", "Fiat", "faralli Mazzanti",
                "Lamborghini", "Maserati", "Pagani", "Iveco", "konicek", "Volvo", "Skoda", "BYD", "Baojun", "BAIC magic speed", "Pentium", "BAIC Shenbao", "BAIC Weiwang",
                "BAIC manufacturing", "Beijing", "BAIC new energy", "Chang'an car", "Great Wall", "Chang'an business", "Changhe", "Chang'an crossing", "Great Wall Huaguan", "success", "Dongfeng Fengxing", "Southeast", "Dongfeng Fengshen", "Dongfeng Xiaokang",
                "Dongfeng · Zhengzhou Nissan", "Dongfeng demeanor", "Dongfeng Yufeng", "Futian", "Fudi", "Fuqi QiTeng", "Feichi business car", "GAC trumpchi", "Guanzhi automobile", "GAC GIO", "GAC Hino", "haver", "Haima",
                "Hongqi", "Huatai", "Huanghai", "Hafei", "Haima commercial vehicle", "Huasong", "Hengtian automobile", "Haige", "Huizhong", "Jianghuai", "Geely Automobile", "Jiangling", "Jinbei", "Jiulong", "Jiangnan", "Jiangling group light automobile",
                "Hongqi", "Huatai", "Huanghai", "Hafei", "Haima commercial vehicle", "Huasong", "Hengtian automobile", "Haige", "Huizhong", "Jianghuai", "Geely Automobile", "Jiangling", "Jinbei", "Jiangnan", "Jiangling group",
                "Jinlv bus", "Kairui", "Kaiyi", "Kawei", "Corys", "Kangdi", "Lufeng", "cheetah", "Lifan", "Lianhua", "Blue Ocean RV", "concept", "reading electric", "mg", "Meiya", "nazhijie",
                "Oulang", "OULIAN", "Chery", "Qichen", "Qingling", "Rongwei", "SAIC Maxus", "SAIC Tongjia", "Tengshi", "Wuling", "Weichai Yingzhi", "Weilin", "Weichai Ourui", "Xiamen Jinlong",
                "Xinkai", "FAW", "YEMA", "Yongyuan", "Ranger", "Yutong", "Yujie", "Yangzhou Yaxing bus", "Zhongtai", "Zhonghua", "ZTE", "Zhidou", "zhongeurope Benz RV", "zhinuo", "Zhejiang Carlson",
                "Zhongtong bus", "sinotruk trump"));

        this.setVehicleBrandDesc(RandomDataUtil.randomValue("奥迪", "AC Schnitzer", "Artega", "奔驰", "宝马", "保时捷", "巴博斯", "宝沃", "大众",
                "KTM", "卡尔森bai", "MINI", "欧宝", "smart", "STARTECH", "泰卡特", "威兹曼", "西雅特", "日韩品牌：", "本田", "丰田", "光冈", "铃木", "雷克萨斯",
                "朗世", "马自达", "讴歌", "日产", "三菱", "斯巴鲁", "五十铃", "英菲尼迪", "起亚", "双龙", "现代", "别克", "道奇", "福特", "GMC",
                "Jeep", "凯迪拉克", "克莱斯勒", "林肯", "乔治·巴顿", "山姆", "特斯拉", "雪佛兰", "星客特", "欧系其他：", "标致", "DS", "雷诺", "PGO", "雪铁龙",
                "阿斯顿·马丁", "宾利", "捷豹", "路虎", "劳斯莱斯", "路特斯", "迈凯伦", "摩根", "Noble", "阿尔法·罗密欧", "布加迪", "法拉利", "菲亚特", "Faralli Mazzanti",
                "兰博基尼", "玛莎拉蒂", "帕加尼", "依维柯", "科尼赛克", "沃尔沃", "斯柯达", "比亚迪", "宝骏", "北汽幻速", "奔腾", "北汽绅宝", "北汽威旺",
                "北汽制造", "北京", "北汽新能源", "长安轿车", "长城", "长安商用", "昌河", "长安跨越", "长城华冠", "成功", "东风风行", "东南", "东风风神", "东风小康",
                "东风·郑州日产", "东风风度", "东风御风", "福田", "福迪", "福汽启腾", "飞驰商务车", "广汽传祺", "观致汽车", "广汽吉奥", "广汽日野", "哈弗", "海马",
                "红旗", "华泰", "黄海", "哈飞", "海马商用车", "华颂", "恒天汽车", "海格", "汇众", "江淮", "吉利汽车", "江铃", "金杯", "九龙", "江南", "江铃集团轻汽",
                "金旅客车", "开瑞", "凯翼", "卡威", "科瑞斯的", "康迪", "陆风", "猎豹汽车", "力帆", "莲花", "蓝海房车", "理念", "雷丁电动", "MG", "美亚", "纳智捷",
                "欧朗", "欧联", "奇瑞", "启辰", "庆铃", "荣威", "上汽大通MAXUS", "上喆汽车", "陕汽通家", "腾势", "五菱", "潍柴英致", "威麟", "潍柴欧睿", "厦门金龙",
                "新凯", "一汽", "野马汽车", "永源", "游侠汽车", "宇通", "御捷", "扬州亚星客车", "众泰", "中华", "中兴", "知豆", "中欧奔驰房车", "之诺", "浙江卡尔森",
                "中通客车", "重汽王牌"));

        this.setVehicleClass(RandomDataUtil.randomValue("Heavy duty full trailer", "small car", "mini car", "compact car", "medium car", "advanced car",
                "Hatchback", "MPV", "SUV", "CDV"));
        this.setVehicleClassDesc(RandomDataUtil.randomValue("重型全挂车", "小型车", "微型车", "紧凑车型", "中等车型", "高级车型",
                "三厢车型", "MPV车型", "SUV等车型", "CDV车型"));

        this.setVehicleColor(RandomDataUtil.randomValue("Silver gray", "white", "black", "yellow", "blue", "gray", "red", "Brown", "pink"));
        this.setVehicleColorDesc(RandomDataUtil.randomValue("银灰色", "白色", "黑色", "黄色", "蓝色", "灰色", "红色", "棕色", "粉红色"));
        /**
         * row key设计
         */
        this.setRowkey(RandomDataUtil.stringToMD5(this.getPlateNo()).substring(0, 6) + "-" + this.getPlateNo().substring(1, 7));

    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
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
}
