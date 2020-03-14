package com.sixestates.crawler.infoprovider.exception.common;

import com.sixestates.crawler.infoprovider.exception.BaseServerException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * @author maximin
 */
@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class InternalException extends BaseServerException {

    public InternalException(Throwable cause) {
        super(cause, BaseServerException.INVALID_REQUEST_DATA, HttpStatus.INTERNAL_SERVER_ERROR,
                "Internal error",
                cause.getMessage());
    }

    public InternalException(String errMsg) {
        super(BaseServerException.INTERNAL_ERROR, HttpStatus.INTERNAL_SERVER_ERROR,
                "Internal error", errMsg);
    }
}

