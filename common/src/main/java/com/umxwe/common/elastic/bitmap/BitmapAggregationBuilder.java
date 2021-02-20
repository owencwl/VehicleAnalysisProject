/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.umxwe.common.elastic.bitmap;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
//import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceParseHelper;
import org.elasticsearch.search.internal.SearchContext;

public class BitmapAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<Bytes, BitmapAggregationBuilder> {

    public static final String NAME = "bitmap";

    private static final ObjectParser<BitmapAggregationBuilder, String> PARSER;

    static {
        PARSER = ObjectParser.fromBuilder(BitmapAggregationBuilder.NAME,BitmapAggregationBuilder::new);
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        BitmapAggregatorFactory.registerAggregators(builder);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new BitmapAggregationBuilder(aggregationName), null);
    }

    public BitmapAggregationBuilder(BitmapAggregationBuilder bitmapAggregationBuilder, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(bitmapAggregationBuilder,factoriesBuilder,metadata);
    }
    public BitmapAggregationBuilder(String name) {
        super(name);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new BitmapAggregationBuilder(this,factoriesBuilder,metadata);
    }

    /**
     * Read from a stream.
     */
    public BitmapAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config, AggregatorFactory parent, Builder subFactoriesBuilder) throws IOException {
        return new BitmapAggregatorFactory(name, config, queryShardContext, parent, subFactoriesBuilder,metadata);
    }


    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

}
