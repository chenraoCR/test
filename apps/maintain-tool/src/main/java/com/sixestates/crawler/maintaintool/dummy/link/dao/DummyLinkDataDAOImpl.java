package com.sixestates.crawler.maintaintool.dummy.link.dao;

import com.lakeside.core.utils.GZipUtil;
import com.sixestates.crawler.maintaintool.dummy.link.DummyLinkData;
import com.sixestates.crawler.maintaintool.dummy.link.DummyLinkDataDAO;
import com.sixestates.crawler.util.mysql.MySQLBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DummyLinkDataDAOImpl extends MySQLBase implements DummyLinkDataDAO {
    private final String sc_table = "sc_dummy_data";

    @Override
    public int insert(DummyLinkData dummyLinkData) {
        StringBuilder builder = new StringBuilder()
                .append(" INSERT INTO ").append(sc_table)
                .append(" (`key`, `raw_page`) ")
                .append(" VALUES (:key, :raw_page) ")
                .append(" ON DUPLICATE KEY UPDATE ")
                .append(" `raw_page`=VALUES(`raw_page`) ");

        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("key", dummyLinkData.getKey());
        try {
        	byte data[] = dummyLinkData.getRawPage().getBytes("utf-8");
        	data = GZipUtil.compress(data);
			paramsMap.put("raw_page", data);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("UnsupportedEncodingException");
		}
        return dataSource.getJdbcTemplate().update(builder.toString(), paramsMap);
    }
}
