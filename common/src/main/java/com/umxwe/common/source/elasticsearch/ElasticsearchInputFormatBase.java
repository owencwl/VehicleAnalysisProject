package com.umxwe.common.source.elasticsearch;

/**
 * @ClassName ElasticsearchInputFormatBase
 * @Description Todo
 * @Author owen(umxwe))
 * @Date 2020/12/17
 */

import com.umxwe.common.param.Params;
import com.umxwe.common.source.elasticsearch.param.ElasticSearchSourceParams;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * 本抽象类提供ES的batch读取，而stream的方式目前flink还没有实现方案，初步使用translog来监听es形成stream
 *
 * @param <IN> 读取es数据转化为IN，一般使用org.apache.flink.types.Row
 */
public abstract class ElasticsearchInputFormatBase<IN> extends RichInputFormat<IN, GenericInputSplit> implements ResultTypeQueryable<Row> {
    private final static Logger logger = LoggerFactory.getLogger(ElasticsearchInputFormatBase.class);

    //-----所有的未在open方法中初始化的属性，都应可序列化，不能序列化的应该用transient修饰-----//

    /**
     * 使用RestHighLevelClient进行查询读取数据
     */
    private transient RestHighLevelClient client;

    /**
     * ES的游标,每次请求都要带上
     */
    private transient Scroll scroll;

    /**
     * 每次请求的回复。 当本次的回复的所有hits都被遍历完后，调用nextSearchResponse()获取下一个
     */
    private transient SearchResponse searchResponse;

    /**
     * 通过searchResponse获取的当次请求的hits的迭代器
     */
    private transient Iterator<SearchHit> searchHitIterator;
    /**
     * 配置参数
     */
    private Params ESParams;
    /**
     * es 查询条件转换之后的字符串
     */
    private String queryConditionStr;

    /**
     * 字段元数据信息
     */
    private RowTypeInfo rowTypeInfo;

    /**
     * 用来转换为Row
     */
    public Map<String, Integer> position;


    ElasticsearchInputFormatBase(Params ESParams, String queryConditionStr, RowTypeInfo rowTypeInfo) {
        this.queryConditionStr = queryConditionStr;
        this.ESParams = ESParams;
        this.rowTypeInfo = rowTypeInfo;
    }

    /**
     * 根据split初始化这个分区的所有参数，这里的split和分区的概念差不多，split的总数即为并行度数量
     *
     * @param split 当前打开的split，可以从中获取split总数和当前的split数
     * @throws IOException client查询时有可能会抛出该异常
     */
    @Override
    public void open(GenericInputSplit split) throws IOException {
        this.client = new RestHighLevelClient(clientBuilder());
        this.scroll = scroll();
        position = new CaseInsensitiveMap();

        if (rowTypeInfo != null) {
            int i = 0;
            for (String name : rowTypeInfo.getFieldNames()) {
                position.put(name, i++);
            }
        }
        //第一次搜索请求的回复及hits迭代器
        this.searchResponse = this.client.search(initSearchRequest(this.scroll, split), RequestOptions.DEFAULT);
        this.searchHitIterator = this.searchResponse.getHits().iterator();
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] inputSplits) {
//        return new LocatableInputSplitAssigner(inputSplits);
        return new DefaultInputSplitAssigner(inputSplits);

    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    /**
     * refer to GenericInputFormat.java
     *
     * @param numSplits
     * @return
     * @throws IOException
     */
    @Override
    public GenericInputSplit[] createInputSplits(int numSplits) throws IOException {
        if (numSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }
        numSplits = (this instanceof NonParallelInput) ? 1 : numSplits;
        GenericInputSplit[] splits = new GenericInputSplit[numSplits];
        for (int i = 0; i < splits.length; i++) {
            splits[i] = new GenericInputSplit(i, numSplits);
        }
        return splits;
    }

    /**
     * 根据配置得到RestHighLevelClient
     *
     * @return RestHighLevelClient
     */
    private RestClientBuilder clientBuilder() {
//        ArrayList<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
        List<HttpHost> httpHosts = Arrays.asList(ESParams.get(ElasticSearchSourceParams.HTTPHOSTS).split(","))
                .stream()
                .map(host -> new HttpHost(host, 9200, "http"))
                .collect(Collectors.toList());

        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
//        authenticate(builder);
        return builder;
    }

    /**
     * 有的client有用户名和密码，需要在builder中进行设置
     *
     * @param builder RestClientBuilder
     */
//    private void authenticate(RestClientBuilder builder) {
//        //如果配置了用户名和密码，则需要如下的配置
//            String username = "";
//            String password = "";
//            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
//            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
//    }

    /**
     * 通过配置得到游标Scroll
     *
     * @return Scroll
     */
    private Scroll scroll() {
        return new Scroll(this.ESParams.get(ElasticSearchSourceParams.SCROLLTIME));
    }


    /**
     * 根据配置以及游标，得到初始的SearchRequest
     *
     * @param scroll 查询时的游标
     * @param split  本次开启的split
     * @return SearchRequest
     */
    private SearchRequest initSearchRequest(Scroll scroll, GenericInputSplit split) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        if ("".equals(queryConditionStr) || queryConditionStr == null) {
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        } else {
            searchSourceBuilder.query(QueryBuilders.wrapperQuery(queryConditionStr));
        }

        if (rowTypeInfo != null) {
            searchSourceBuilder.fetchSource(rowTypeInfo.getFieldNames(), null);
        }

        // 批次大小设置
        searchSourceBuilder.size(this.ESParams.get(ElasticSearchSourceParams.BATCHSIZE));

        //这里可以直接根据split的配置进行分片的读取，最好按照es的分片数进行分区, 这里得split 继承于GenericInputSplit.class
        //详见https://www.elastic.co/guide/en/elasticsearch/reference/7.1/search-request-scroll.html#sliced-scroll
        searchSourceBuilder.slice(
                new SliceBuilder(split.getSplitNumber(), //得到当前分区数
                        split.getTotalNumberOfSplits() //得到总分区数
                )
        );
        //
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(this.ESParams.get(ElasticSearchSourceParams.INDEX));
        searchRequest.scroll(scroll);

        //elasticsearch可以使用preference参数来指定分片查询的优先级，即我们可以通过该参数来控制搜索时的索引数据分片。
        //        searchRequest.preference("_shards:" + split.getShard());

        searchRequest.source(searchSourceBuilder);

        return searchRequest;
    }

    /**
     * 判断是否已到最后一项
     *
     * @return
     * @throws IOException
     */
    @Override
    public boolean reachedEnd() throws IOException {
        if (this.searchHitIterator.hasNext()) {
            return false;
        } else {
            nextSearchResponse();
            return !this.searchHitIterator.hasNext();
        }
    }

    /**
     * 当上一次请求得到的数据全部被消费完后，通过client从scroll中进行下一次请求
     *
     * @throws IOException searchScroll可能抛出IO异常
     */
    private void nextSearchResponse() throws IOException {
        String scrollId = this.searchResponse.getScrollId();
        //新建通过游标搜索的请求
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(this.scroll);
        this.searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
        this.searchHitIterator = this.searchResponse.getHits().iterator();
    }

    /**
     * 返回下一条记录，同时可以提供格式转换功能，需自定义实现
     *
     * @param reuse
     * @return
     * @throws IOException
     */
    @Override
    public IN nextRecord(IN reuse) throws IOException {
        return transform(this.searchHitIterator.next(), reuse);
    }

    /**
     * 将searchHit转化为需要的数据结构
     *
     * @param hit SearchHit
     * @return IN 用户自行定义的数据结构
     */
    public abstract IN transform(SearchHit hit, IN reuse) throws IOException;

    @Override
    public TypeInformation<Row> getProducedType() {
        return this.rowTypeInfo;
    }

    /**
     * 关闭es的连接
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (this.client != null) {
            clearScroll();
            this.client.close();
            this.client = null;
        }
    }

    /**
     * 清除es滚动查询记录
     *
     * @throws IOException
     */
    private void clearScroll() throws IOException {
        if (this.searchResponse.getScrollId() == null) {
            return;
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(this.searchResponse.getScrollId());
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        logger.info("Clear scroll response:{}", succeeded);
    }
}
