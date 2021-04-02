package com.umxwe.genetedata.tmp;

/**
 * @ClassName TimeRange
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/8
 */
public class TimeRange {

    private String startPoint;
    private String endPoint;

    public String getStartPoint() {
        return startPoint;
    }

    public void setStartPoint(String startPoint) {
        this.startPoint = startPoint;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String toString() {
        return startPoint + " - " + endPoint;
    }
}