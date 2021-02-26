package com.umxwe.common.elastic.distance;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ArrayValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ArrayValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.aggregations.support.ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD;


/**
 * @ClassName UmxDistanceParser
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/24
 */
public class UmxDistanceParser extends ArrayValuesSourceParser.NumericValuesSourceParser {

    public UmxDistanceParser() {
        super(true);
    }

    @Override
    protected ArrayValuesSourceAggregationBuilder<?> createFactory(String aggregationName, ValuesSourceType valuesSourceType, ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        UmxDistanceAggregationBuilder builder = new UmxDistanceAggregationBuilder(aggregationName);
        String mode = (String)otherOptions.get(MULTIVALUE_MODE_FIELD);
        if (mode != null) {
            builder.multiValueMode(MultiValueMode.fromString(mode));
        }
        return builder;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, XContentParser.Token token, XContentParser parser, Map<ParseField, Object> otherOptions) throws IOException {
        if (MULTIVALUE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            if (token == XContentParser.Token.VALUE_STRING) {
                otherOptions.put(MULTIVALUE_MODE_FIELD, parser.text());
                return true;
            }
        }
        return false;
    }
}
