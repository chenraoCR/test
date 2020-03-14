package com.sixestates.crawler.infoprovider.exception.common;

import com.sixestates.crawler.infoprovider.exception.BaseServerException;
import org.springframework.http.HttpStatus;

/**
 * Created by @maximin.
 */
public class DataSearchException extends BaseServerException {

    public DataSearchException() {
        super(BaseServerException.DATA_SEARCH_ERROR, HttpStatus.NOT_FOUND, "Data Search Error", "Data Search Error");
    }

    public DataSearchException(String errMsg) {
        super(BaseServerException.DATA_SEARCH_ERROR, HttpStatus.NOT_FOUND, "Data Search Error", errMsg);
    }
}
