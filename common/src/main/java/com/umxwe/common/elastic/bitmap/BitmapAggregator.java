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
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BitmapAggregator extends NumericMetricsAggregator.SingleValue {

    private static final Logger LOG = LoggerFactory.getLogger(BitmapAggregator.class);

    final ValuesSource.Bytes valuesSource;
    final DocValueFormat format;

    ObjectArray<RoaringBitmap> sums;

//    public BitmapAggregator(
//            String name, ValuesSource.Bytes valuesSource, DocValueFormat formatter, SearchContext context,
//            Aggregator parent,Map<String, Object> metadata) throws IOException {
//        super(name, context, parent,metadata);
//        this.valuesSource = valuesSource;
//        this.format = formatter;
//        if (valuesSource != null) {
//            sums = context.bigArrays().newObjectArray(1);
//        }
//    }

    public BitmapAggregator(String name, ValuesSourceConfig valuesSourceConfig, SearchContext searchContext, Aggregator aggregator, Map<String, Object> stringObjectMap) throws IOException {
        super(name,searchContext,aggregator,stringObjectMap);
        this.format =valuesSourceConfig.format();
        this.valuesSource = valuesSourceConfig.hasValues()? (ValuesSource.Bytes) valuesSourceConfig.getValuesSource() :null;
        if (valuesSource != null) {
            sums = context.bigArrays().newObjectArray(1);
        }
    }

//    @Override
//    public boolean needsScores() {
//        return valuesSource != null && valuesSource.needsScores();
//    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = bigArrays.grow(sums, bucket + 1);
               if( values.advanceExact(doc)){
                   final int valuesCount = values.docValueCount();
                   RoaringBitmap sum = new RoaringBitmap();
                   for (int i = 0; i < valuesCount; i++) {
                       BytesRef valueAt = values.nextValue();
                       byte[] bytes = valueAt.bytes;
                       RoaringBitmap roaringBitmap = BitmapUtil.deserializeBitmap(bytes);
                       sum.or(roaringBitmap);
                   }
                   RoaringBitmap roaringBitmap = sums.get(bucket);
                   if (roaringBitmap == null) {
                       sums.set(bucket, sum);
                   } else {
                       roaringBitmap.or(sum);
                   }
                   LOG.debug("LeafBucketCollectorBase collect: doc [{}], bucket [{}], bitmap [{}]", doc, bucket, sums.get(bucket));
               }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        LOG.debug("BitmapAggregator metric: owningBucketOrd [{}]", owningBucketOrd);
        if (valuesSource == null || owningBucketOrd >= sums.size()) {
            return 0.0;
        }
        return sums.get(owningBucketOrd).getCardinality();
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        LOG.debug("BitmapAggregator buildAggregation: bucket [{}], sum [{}]", bucket, this.sums);
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalBitmap(name, sums.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        LOG.debug("BitmapAggregator buildEmptyAggregation");
        return new InternalBitmap(name, new RoaringBitmap(), format, metadata());
    }

    @Override
    public void doClose() {
        LOG.debug("BitmapAggregator doClose");
        Releasables.close(sums);
    }

}
