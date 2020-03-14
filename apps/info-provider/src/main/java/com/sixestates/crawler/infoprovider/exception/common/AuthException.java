package com.sixestates.crawler.infoprovider.exception.common;

import com.sixestates.crawler.infoprovider.exception.BaseServerException;
import org.springframework.http.HttpStatus;

/**
 * Created by @maximin.
 */
public class AuthException extends BaseServerException {

    public AuthException() {
        super(BaseServerException.AUTH_FAIL, HttpStatus.UNAUTHORIZED, "Auth failed", "Auth failed");
    }

    public AuthException(String errMsg) {
        super(BaseServerException.AUTH_FAIL, HttpStatus.UNAUTHORIZED, "Auth failed", errMsg);
    }
}
