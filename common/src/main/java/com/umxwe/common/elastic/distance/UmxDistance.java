package com.umxwe.common.elastic.distance;

import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

public interface UmxDistance extends NumericMetricsAggregation.SingleValue {

    double getValue();

}
