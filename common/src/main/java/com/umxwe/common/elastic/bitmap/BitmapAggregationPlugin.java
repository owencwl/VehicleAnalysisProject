package com.umxwe.common.elastic.bitmap;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregator;

public class BitmapAggregationPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<AggregationSpec> getAggregations() {
        return Collections.singletonList(new AggregationSpec(BitmapAggregationBuilder.NAME, BitmapAggregationBuilder::new, BitmapAggregationBuilder::parse)
                .addResultReader(InternalBitmap::new));
    }
}
