package com.umxwe.common.elastic;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @ClassName ElasticUtil
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
public class ElasticUtil {

//    public static <T>  void batchInsert(RestHighLevelClient client, String indexName , List<T> lists) throws IOException {
//        // TODO Auto-generated method stub
//        BulkRequest request = new BulkRequest();
//
//        for (T product : lists) {
//            Map<String,Object> m  = product.toMap();
//            IndexRequest indexRequest= new IndexRequest(indexName, indexName, String.valueOf(product.getId())).source(m);
//            request.add(indexRequest);
//        }
//
//        client.bulk(request);
//        System.out.println("批量插入完成");
//    }

//    public static void deleteDocument(int id) throws IOException {
//        DeleteRequest deleteRequest = new DeleteRequest (indexName,"product", String.valueOf(id));
//        client.delete(deleteRequest);
//        System.out.println("已经从ElasticSearch服务器上删除id="+id+"的文档");
//    }
//
//    public static void updateDocument(Product product) throws IOException {
//
//        UpdateRequest  updateRequest = new UpdateRequest (indexName, "product", String.valueOf(product.getId()))
//                .doc("name",product.getName());
//
//        client.update(updateRequest);
//        System.out.println("已经在ElasticSearch服务器修改产品为："+product);
//
//    }
//
//    public static void getDocument(int id) throws IOException {
//        // TODO Auto-generated method stub
//        GetRequest request = new GetRequest(
//                indexName,
//                "product",
//                String.valueOf(id));
//
//        GetResponse response = client.get(request);
//
//        if(!response.isExists()){
//            System.out.println("检查到服务器上 "+"id="+id+ "的文档不存在");
//        }
//        else{
//            String source = response.getSourceAsString();
//            System.out.print("获取到服务器上 "+"id="+id+ "的文档内容是：");
//
//            System.out.println(source);
//
//        }
//
//
//
//
//    }
//
//    public static void addDocument(Product product) throws IOException {
//        Map<String, Object> jsonMap = new HashMap<>();
//        jsonMap.put("name", product.getName());
//        IndexRequest indexRequest = new IndexRequest(indexName, "product", String.valueOf(product.getId()))
//                .source(jsonMap);
//        client.index(indexRequest);
//        System.out.println("已经向ElasticSearch服务器增加产品："+product);
//    }
//
//    public static boolean checkExistIndex(String indexName) throws IOException {
//        boolean result =true;
//        try {
//
//            OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
//            client.indices().open(openIndexRequest).isAcknowledged();
//
//        } catch (ElasticsearchStatusException ex) {
//            String m = "Elasticsearch exception [type=index_not_found_exception, reason=no such index]";
//            if (m.equals(ex.getMessage())) {
//                result = false;
//            }
//        }
//        if(result)
//            System.out.println("索引:" +indexName + " 是存在的");
//        else
//            System.out.println("索引:" +indexName + " 不存在");
//
//        return result;
//
//    }
//
//    public static void deleteIndex(String indexName) throws IOException {
//        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
//        client.indices().delete(request);
//        System.out.println("删除了索引："+indexName);
//
//    }
//
//    public static void createIndex(String indexName) throws IOException {
//        // TODO Auto-generated method stub
//        CreateIndexRequest request = new CreateIndexRequest(indexName);
//        client.indices().create(request);
//        System.out.println("创建了索引："+indexName);
//    }
}
