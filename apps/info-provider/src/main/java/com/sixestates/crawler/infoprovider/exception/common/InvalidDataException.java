package com.sixestates.crawler.infoprovider.exception.common;

import com.sixestates.crawler.infoprovider.exception.BaseServerException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * @author maximin
 */
@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class InvalidDataException extends BaseServerException {

    public InvalidDataException() {
        super(BaseServerException.INVALID_REQUEST_DATA, HttpStatus.BAD_REQUEST,
                "Invalid Request Data",
                "Invalid Request Data");
    }

    public InvalidDataException(String errMsg) {
        super(BaseServerException.INVALID_REQUEST_DATA, HttpStatus.BAD_REQUEST,
                "Invalid Data", errMsg);
    }
}

