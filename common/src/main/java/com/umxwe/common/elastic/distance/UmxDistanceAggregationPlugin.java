package com.umxwe.common.elastic.distance;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * @ClassName UmxDistanceAggregationPlugin
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/24
 */
public class UmxDistanceAggregationPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<AggregationSpec> getAggregations() {
        return singletonList(new AggregationSpec(UmxDistanceAggregationBuilder.NAME, UmxDistanceAggregationBuilder::new,
                new UmxDistanceParser()).addResultReader(InternalUmxDistance::new));
    }
}
