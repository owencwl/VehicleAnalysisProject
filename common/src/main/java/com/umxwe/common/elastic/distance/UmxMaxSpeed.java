package com.umxwe.common.elastic.distance;

import org.elasticsearch.search.aggregations.Aggregation;

public interface UmxMaxSpeed extends Aggregation {

    /**
     * return the total document count
     */
    long getDocCount();

    /**
     * return the max speed
     */
    double getMaxSpeed();
}
