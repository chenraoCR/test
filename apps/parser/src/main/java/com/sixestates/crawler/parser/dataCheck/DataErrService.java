package com.sixestates.crawler.parser.dataCheck;

import com.sixestates.crawler.datastorage.mysql.datasaver.DataSaverErrDAO;
import com.sixestates.crawler.datastorage.mysql.datasaver.impl.DataSaverErrDAOImpl;
import com.sixestates.crawler.model.external.datasaver.dao.impl.ExternalDataSaverErrDAOImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DataErrService {

    private static DataSaverErrDAO extDataSaverErrDAO = new ExternalDataSaverErrDAOImpl();
    private static DataSaverErrDAO dataSaverErrDAO = new DataSaverErrDAOImpl();

    public static boolean checkData(Map<String, Map<String, Object>> row, String table, boolean isExternal){

        int status = DataCheckUtil.checkData(row, table);
        /*int status = DataCheckUtil.mockErrorCheckData(row, table);*/

        if(status == DataCheckUtil.STATUS_VALID){
            return true;
        }

        (isExternal ? extDataSaverErrDAO : dataSaverErrDAO)
                .save(row,status);
        return false;
    }



}
