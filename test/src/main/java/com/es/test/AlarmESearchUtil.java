package com.es.test;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.util.List;
import java.util.Map;

public class AlarmESearchUtil {


    public Map<String, Object> getAlarmByEntity(String entity){
        ESUtils esUtils = new ESUtils("192.168.1.101", "test", "msg" );
        QueryBuilder queryBuilder = QueryBuilders.termQuery("entity", entity);
        List<Map<String, Object>> maps = esUtils.queryDataList(queryBuilder);
        if (maps != null && maps.size() > 0){
            return maps.get(0);
        }
        return null;
    }

    public Map<String, Object> getAlarmById(Integer id){
        ESUtils esUtils = new ESUtils("192.168.1.101", "test", "msg" );
        Map<String, Object> map = esUtils.getDataResponse(id + "");
        if (map != null && map.size() > 0) return map;
        return null;
    }

    public List<Map<String, Object>> getAlarmList(List<String> resTypes, String sortId, String pid, String level,
                                                  Integer periodType, Long start, Long end, String allAlarmType, Integer pageStart, Integer pageLength) throws Exception {
        ESUtils esUtils = new ESUtils("192.168.1.101", "test", "msg" );

        Integer rule1 = -1;
        Integer rule2 = -1;
        if ("1".equals(allAlarmType)) {// 当前告警
            rule1 = 1;
            rule2 = 2;
        } else if("4".equals(allAlarmType)){
            rule1 = 4;
            rule2 = 4;
        }else {// 历史告警
            rule1 = 2;
            rule2 = 3;
        }
        QueryBuilder queryBuilder = QueryBuilders.boolQuery();
        if (resTypes != null && resTypes.size() > 0 && !StringUtils.isEmpty(sortId) && null == periodType) {
            if (StringUtils.isEmpty(level)) {
                QueryBuilder qb1 = QueryBuilders.boolQuery();
                for (String resType : resTypes){
                    QueryBuilder qb2 = QueryBuilders.termQuery("resType", resType);
                    ((BoolQueryBuilder) qb1).should(qb2);
                }
                ((BoolQueryBuilder) queryBuilder).must(qb1);
                QueryBuilder qb3 = QueryBuilders.boolQuery();
                ((BoolQueryBuilder) qb3).should(QueryBuilders.termQuery("option",rule1)).should(QueryBuilders.termQuery("option",rule2));
                ((BoolQueryBuilder) queryBuilder).must(qb3);
            }
        }
        List<Map<String, Object>> maps = esUtils.queryDataList(queryBuilder);
        if (maps != null) return maps;
        return null;
    }

}
