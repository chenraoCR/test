package com.sixestates.crawler.infoprovider.mvc.dataflow;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.framework.config.serialize.HessianGzipSerializer;
import com.framework.fdfs.FdfsClient;
import com.framework.fdfs.FdfsClientFactory;
import com.framework.fdfs.FdfsException;
import com.lakeside.core.utils.GZipUtil;
import com.sixestates.crawler.config.CommonConfig;
import com.sixestates.crawler.datastorage.mysql.datasaver.*;
import com.sixestates.crawler.datastorage.mysql.datasaver.impl.DataSaverDAOImplMySQL;
import com.sixestates.crawler.datastorage.mysql.datasaver.impl.DataSaverErrDAOImpl;
import com.sixestates.crawler.infoprovider.exception.common.DataSearchException;
import com.sixestates.crawler.infoprovider.model.Page;
import com.sixestates.crawler.model.batch.Batch;
import com.sixestates.crawler.model.batch.BatchDAO;
import com.sixestates.crawler.model.batch.impl.BatchDAOImplMySQL;
import com.sixestates.crawler.model.crawlurl.CrawlUrl;
import com.sixestates.crawler.model.crawlurl.CrawlUrlDAO;
import com.sixestates.crawler.model.crawlurl.impl.CrawlUrlDAOImplMySQL;
import com.sixestates.crawler.model.external.batch.ExternalBatch;
import com.sixestates.crawler.model.external.batch.dao.ExternalBatchDAO;
import com.sixestates.crawler.model.external.batch.dao.impl.ExternalBatchDAOImplMySQL;
import com.sixestates.crawler.model.external.datasaver.dao.impl.ExternalDataSaverErrDAOImpl;
import com.sixestates.crawler.model.uploadpack.UploadPack;
import com.sixestates.crawler.model.uploadpack.UploadPackDAO;
import com.sixestates.crawler.model.uploadpack.impl.UploadPackDAOImplMySQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by @maximin.
 */
public class DataFlowService {

    private static final Logger logger = LoggerFactory.getLogger(DataFlowService.class);
    private static final CrawlUrlDAO crawlUrlDao = new CrawlUrlDAOImplMySQL();
    private static final DataSaverDAO internalDataSaverDao = new DataSaverDAOImplMySQL();
    private static final DataSaverErrDAO internalDataSaverErrDAO  = new DataSaverErrDAOImpl();
    private static final DataSaverErrDAO externalDataSaverErrDAO  = new ExternalDataSaverErrDAOImpl();
    private static final BatchDAO batchDAO  = new BatchDAOImplMySQL();
    private static final ExternalBatchDAO externalBatchDAO = new ExternalBatchDAOImplMySQL();
    private static final UploadPackDAO uploadPackDAO = new UploadPackDAOImplMySQL();
    private static final HessianGzipSerializer gseri = new HessianGzipSerializer();
    private static final FdfsClient fclient = FdfsClientFactory.get(CommonConfig.getFdfs_Cluster_Name());

    public static Map<String,Object> fieldMap=new HashMap<String, Object>();

    static{
        DataSaver data = new DataSaver();
        Field[] fields = data.getClass().getDeclaredFields();
        for(Field field:fields){
            field.setAccessible(true);
            try {
                fieldMap.put(field.getName(), field.get(data));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        fieldMap.put("errCode",null);
    }

    static List<CrawlUrl> getCrawlURL(Long batchId, String url) {
        return crawlUrlDao.queryCrawlByURL(batchId, url);
    }

    static Page getCrawlWebpage(Long crawlURLId) {
        CrawlUrl crawlUrl = crawlUrlDao.query(crawlURLId);
        UploadPack uploadPack = uploadPackDAO.query(crawlUrl.getUploadPackId());
        byte[] content = null;
        for (int i = 0; i < 3; i++) {
            try {
                logger.info("downloading file");
                content = fclient.download(uploadPack.getPath());
                break;
            } catch (FdfsException e) {
                if (i == 2) {
                    logger.error("{}", e);
                    throw new DataSearchException("Fdfs download failed");
                }
            }
        }
        try {
            List<Page> pages = JSON.parseArray(GZipUtil.uncompress(content), Page.class);
            if (pages == null) {
                throw new DataSearchException(String.format("upload pack that crawl url %d refers to is invalid json array", crawlURLId));
            }
            for (Page page : pages) {
                if (page.getId().equals(crawlURLId)) {
                    return page;
                }
            }
            throw new DataSearchException(String.format("crawl url %d not in its own upload pack", crawlURLId));
        } catch (IOException e) {
            logger.error("{}", e);
            throw new DataSearchException("Uncompress failed");
        }
    }


    static List<Map<String, Object>> getInternalDataSaver(long batchId, long crawlURLId, String extra) {
        List<Map<String, Object>> list = new ArrayList<>();
        List<DataSaver> dataSavers = internalDataSaverDao.queryByFilter(batchId, crawlURLId, extra);
        List<DataSaverError> dataSaverErrors = internalDataSaverErrDAO.queryByFilter(batchId, crawlURLId);
        dataSavers.forEach(a -> {
            list.add(dataSaver2Map(a));
        });
        dataSaverErrors.forEach(a -> {
            list.add(dataSaver2Map(a));
        });
        return list;
    }

    static Map<String, Object> dataSaver2Map(Object obj){
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.putAll(fieldMap);
        if(obj == null){
            return resultMap;
        }

        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(obj);
        for (String key : jsonObject.keySet()){
            resultMap.put(key, jsonObject.get(key));
        }
        return resultMap;
    }


    static DataSaver getInternalDataSaver(Long batchId, Long dataId) {
        Optional<DataSaver> optional = internalDataSaverDao.queryById(batchId, dataId);
        if (optional.isPresent()) {
            return optional.get();
        } else {
            throw new DataSearchException(String.format("No data saver found in internal batch %d and data id = %d", batchId, dataId));
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Map<String, Object>> getInternalDataSaverContent(Long batchId, Long dataId) {
        Optional<DataSaver> optional = internalDataSaverDao.queryById(batchId, dataId);
        if (optional.isPresent()) {
            return (HashMap<String, Map<String, Object>>) gseri.byte2Obj(optional.get().getContent());
        } else {
            throw new DataSearchException(String.format("No data saver found in internal batch %d and data id = %d", batchId, dataId));
        }
    }

    static CrawlUrl getCrawlURL(Long crawlURLId) {
        return crawlUrlDao.query(crawlURLId);
    }

    static List<DataErrorInfo> getDataError(long startTime, long endTime) {
        List<DataErrorInfo> list = new ArrayList<>();
        Date start = new Date(startTime);
        Date end = new Date(endTime);

        List<Batch> batches = batchDAO.queryByActiveTime(start);
        batches.forEach(a -> {
            DataErrorInfo dataErrorInfo = internalDataSaverErrDAO.queryErrorCount(a.getId().longValue(), start, end);
            if(dataErrorInfo != null && dataErrorInfo.getErrorCount() > 0){
                list.add(dataErrorInfo);
            }
        });

        return list;
    }

    static List<DataErrorInfo> getDataErrorExternal(long startTime, long endTime) {
        List<DataErrorInfo> list = new ArrayList<>();
        Date start = new Date(startTime);
        Date end = new Date(endTime);

        List<ExternalBatch> batches = externalBatchDAO.queryByActiveTime(start);
        batches.forEach(a -> {
            DataErrorInfo dataErrorInfo = externalDataSaverErrDAO.queryErrorCount(a.getId().longValue(), start, end);
            if(dataErrorInfo != null && dataErrorInfo.getErrorCount() > 0){
                list.add(dataErrorInfo);
            }
        });

        return list;
    }

}
