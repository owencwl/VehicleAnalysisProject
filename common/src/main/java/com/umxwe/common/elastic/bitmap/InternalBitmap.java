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
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalBitmap extends InternalNumericMetricsAggregation.SingleValue implements BitmapDistinct {

    private static final Logger LOG = LoggerFactory.getLogger(BitmapAggregator.class);

    private final RoaringBitmap sum;

    InternalBitmap(String name, RoaringBitmap sum, DocValueFormat formatter,
            Map<String, Object> metaData) {
        super(name, metaData);
        this.sum = sum;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    public InternalBitmap(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        byte[] bytes = in.readByteArray();
        this.sum = BitmapUtil.deserializeBitmap(bytes);
        LOG.debug("InternalBitmap construct: get sum from input stream [{}]", this.sum);
    }


    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        /* todo 这里是不是应该写字节数组 */
        out.writeNamedWriteable(format);
        out.writeByteArray(Objects.requireNonNull(BitmapUtil.serializeBitmap(sum)));
        LOG.debug("InternalBitmap doWriteTo: write sum to output stream [{}]", this.sum);
    }

    @Override
    public String getWriteableName() {
        return BitmapAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public double getValue() {
        LOG.debug("InternalBitmap getValue: sum [{}]", sum);
        return sum == null ? 0 : sum.getCardinality();
    }


    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        RoaringBitmap sum = new RoaringBitmap();
        for (InternalAggregation aggregation : aggregations) {
            sum.or(((InternalBitmap) aggregation).sum);
        }
        LOG.debug("InternalBitmap doReduce: aggregations size [{}], sum [{}]", aggregations.size(), sum);
        return new InternalBitmap(name, sum, format, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        LOG.debug("InternalBitmap doXContentBody: content sum value [{}]", sum.getCardinality());
        builder.field(CommonFields.VALUE.getPreferredName(), sum.getCardinality());
        if (format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(sum.getCardinality()));
        }
        return builder;
    }

}
