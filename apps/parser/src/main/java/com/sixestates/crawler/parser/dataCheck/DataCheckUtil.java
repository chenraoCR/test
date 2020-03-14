package com.sixestates.crawler.parser.dataCheck;

import com.sixestates.bigdata.check.DataCheck;
import com.sixestates.bigdata.check.impl.DataCheckImpl;
import com.sixestates.crawler.datastorage.mysql.ExternalMySQLDao;
import com.sixestates.crawler.model.cache.DataSourceCache;
import com.sixestates.crawler.model.cache.KafkaTopicDaoCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class DataCheckUtil {

    private static final Logger logger = LoggerFactory.getLogger(DataCheckUtil.class);

    private static DataCheck dataCheck = new DataCheckImpl();

    public static final String FILTER_CAUSE = "filter_cause";

    public static final String FILTER_STATUS = "filter_status";

    public static final int STATUS_VALID = 0;

    public static Integer checkData(Map<String, Map<String, Object>> row, String table){
        List<String> listIndex = KafkaTopicDaoCache.getList(true);
        List<String> listNoIndex = KafkaTopicDaoCache.getList(false);
        ConcurrentMap<String, Integer> idMap = DataSourceCache.getIdMap();
        try {
            Map<String,String> result = dataCheck.checkData(row, table, listIndex, listNoIndex, idMap);
            String status = result.get(DataCheckUtil.FILTER_STATUS);
            return Integer.valueOf(status);
        }catch (Exception e){
            logger.error("datacheck call error ! {}", e.getMessage(),e);
            return -1;
        }

    }

    public static Integer mockErrorCheckData(Map<String, Map<String, Object>> row, String table){
        List<String> listIndex = KafkaTopicDaoCache.getList(true);
        List<String> listNoIndex = KafkaTopicDaoCache.getList(false);
        ConcurrentMap<String, Integer> idMap = DataSourceCache.getIdMap();

        int random = new Random().nextInt(20);
        switch (random){
            case 0:
                row = null;
                break;
            case 1:
                table = null;
                break;
            case 2:
                row.put("dcf", new HashMap<String, Object>());
                break;
            case 3:
                row.get("dcf").put("type", null);
                break;
            case 4:
                //type dont match
                table = listIndex.get(1);
                break;
            case 5:
                row.get("dcf").put("source",null);
                break;
            case 6:
                row.get("dcf").put("pub_time",null);
                break;
            case 7:
                row.get("dcf").put("fet_time",null);
                break;
            case 8:
                row.get("dcf").put("pub_time","1a2a3");
                break;
            case 9:
                row.get("dcf").put("fet_time","1a2a3");
            case 10:
                row.get("dcf").put("fet_time",new Date(new Date().getTime() + 1000*60*60*10));
                break;
            case 11:
                row.get("dcf").put("pub_time",new Date(new Date().getTime() + 1000*60*60*10));
                break;
        }

        try {
            Map<String,String> result = dataCheck.checkData(row, table, listIndex, listNoIndex, idMap);
            String status = result.get(DataCheckUtil.FILTER_STATUS);
            return Integer.valueOf(status);
        }catch (Exception e){
            logger.error("datacheck call error ! {}", e.getMessage(),e);
            return -1;
        }

    }
}
