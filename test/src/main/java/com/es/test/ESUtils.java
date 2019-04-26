package com.es.test;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;


public class ESUtils {

    private static TransportClient client = null;

    /**
     * es index
     */
    private  String ES_INDEX = "";
    /**
     * es type
     */
    private  String ES_TYPE  = "";


    public ESUtils(String ip, String index, String type){
        this.ES_INDEX = index;
        this.ES_TYPE = type;
        try {
            getClient(ip);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        ESUtils test = new ESUtils("192.168.1.101", "test", "msg");
//        test.getClient();
//        test.createIndex();
//        QueryBuilder builder = QueryBuilders.matchAllQuery();//全部查询
//        QueryBuilder builder = QueryBuilders.termQuery("postDate", "19-04-115 14:59:04");//有查询条件
//        QueryBuilders.rangeQuery("name").from(1).to(1);
//        TestMessage testMessage = new TestMessage();
//        testMessage.set_id(3l);
//        testMessage.setCount(0);
//        testMessage.setMark(false);
//        InnerClass innerClass = new InnerClass();
//        innerClass.set_id(1);
//        innerClass.setDescribe("描述信息");
//        testMessage.setInnerClass(innerClass);
//        test.createIndex(testMessage.get_id()+"", JSON.toJSONString(testMessage));
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("id").from(2);
        QueryBuilder queryBuilder=QueryBuilders.boolQuery().must(rangeQueryBuilder);//.must(qb5);
        List<Map<String, Object>> maps = test.queryDataList(queryBuilder);
        for (Map<String, Object> map : maps) {
            String json = JSON.toJSONString(map);
            TestMessage testMessage = JSON.parseObject(json, TestMessage.class);
            System.out.println(JSON.toJSON(testMessage));
        }

    }

    public TransportClient getClient (String ip) throws UnknownHostException {
        if (client == null) {
            synchronized (ESUtils.class) {
                Settings settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch-cluster")
                        .put("client.transport.sniff", false).build();
                client = TransportClient.builder().settings(settings).build()
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), 9300));
                System.out.println("启动客户端："+client.nodeName());//启动客户端
            }
        }
        return client;
    }


    /**
     *
     * @param _id ID值
     * @param json 转换的json字符串
     * @return
     */
    public String createIndex(String _id, String json) {

        IndexResponse response = client.prepareIndex(this.ES_INDEX, this.ES_TYPE, _id).setSource(json).get();
        System.out.println(String.format("create index response: %s", response.toString()));
        return response.getId();
    }

    public boolean insertData2ES(Map<String, String> map){

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for(Map.Entry<String, String> entry : map.entrySet()){
            String _id = entry.getKey();
            String json = entry.getValue();
            bulkRequest.add(client.prepareIndex(ES_INDEX, ES_TYPE, _id).setSource(json));
        }
        BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
        return bulkItemResponses.hasFailures();
    }

    public Map<String, Object> getDataResponse(String _id) {
        GetResponse response = client.prepareGet(this.ES_INDEX, this.ES_TYPE, _id).get();
        System.out.println(String.format("get data response: %s", JSON.toJSONString(response.getSource())));
        return response.getSource();
    }

    public List<Map<String, Object>> queryDataList(QueryBuilder queryBuilder) {
        SearchResponse sResponse = client.prepareSearch(this.ES_INDEX).setTypes(this.ES_TYPE).setQuery(queryBuilder).setFrom(0).setSize(1000).execute().actionGet();
        SearchHits hits = sResponse.getHits();
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        SearchHit[] hitArray = hits.hits();
        Map<String, Object> map;
        for (SearchHit hit : hitArray) {
            map = hit.getSource();
//            TestMessage testMessage = new TestMessage();
//            testMessage.setMessage((String) map.get("message"));
//            Integer id = (Integer) map.get("id");
//            testMessage.set_id(id);
//            testMessage.setMark((Boolean) map.get("mark"));
//            testMessage.setCount((Integer) map.get("count"));
            list.add(map);
        }
        System.out.println(String.format("query data count=%s", list.size()));
        return list;
    }

}
