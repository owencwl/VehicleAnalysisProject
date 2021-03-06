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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalBitmap extends InternalNumericMetricsAggregation.SingleValue implements BitmapDistinct {

    private static final Logger LOG = LoggerFactory.getLogger(BitmapAggregator.class);

    private final Roaring64Bitmap sum;

    InternalBitmap(String name, Roaring64Bitmap sum, DocValueFormat formatter,
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
        LOG.info("InternalBitmap construct: get sum from input stream [{}]", this.sum);
    }


    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        /* todo 这里是不是应该写字节数组 */
        out.writeNamedWriteable(format);
        out.writeByteArray(Objects.requireNonNull(BitmapUtil.serializeBitmap(sum)));
        LOG.info("InternalBitmap doWriteTo: write sum to output stream [{}]", this.sum);
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
    public long getValue() {
        LOG.info("InternalBitmap getValue: sum [{}]", sum);
        return sum == null ? 0 : sum.getLongCardinality();
    }

    @Override
    public byte[] getByteValue() {
        return sum == null ? null : BitmapUtil.serializeBitmap(sum);
    }


    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Roaring64Bitmap sum = new Roaring64Bitmap();
        for (InternalAggregation aggregation : aggregations) {
            sum.or(((InternalBitmap) aggregation).sum);
        }
        LOG.info("InternalBitmap doReduce: aggregations size [{}], sum [{}]", aggregations.size(), sum);
        return new InternalBitmap(name, sum, format, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        LOG.info("InternalBitmap doXContentBody: content sum value [{}]", sum.getLongCardinality());
//        List<Long> longList=new ArrayList<>();
//        sum.forEach(new LongConsumer() {
//            @Override
//            public void accept(long value) {
//                longList.add(value);
//            }
//        });
        builder.field(CommonFields.VALUE.getPreferredName(), BitmapUtil.serializeBitmap(sum));
//        builder.field(CommonFields.VALUE.getPreferredName(),longList);
        builder.field("Cardinality", sum.getLongCardinality());
        return builder;
    }

}
