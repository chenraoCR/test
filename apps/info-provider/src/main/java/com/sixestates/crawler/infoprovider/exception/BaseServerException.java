package com.sixestates.crawler.infoprovider.exception;

import com.alibaba.fastjson.JSON;
import org.springframework.http.HttpStatus;

/**
 * @author maximin
 */
public abstract class BaseServerException extends RuntimeException {

    public static final int AUTH_FAIL = 1;
    public static final int INVALID_REQUEST_DATA = 2;
    public static final int INTERNAL_ERROR = 3;
    public static final int DATA_SEARCH_ERROR = 4;

    private int code;

    private HttpStatus status;

    private String name;

    private String description;

    public BaseServerException(Throwable cause, int code, HttpStatus status,
                               String name, String description) {
        super(name, cause);

        this.code = code;
        this.status = status;
        this.name = name;
        this.description = description;
    }

    public BaseServerException(int code, HttpStatus status, String name,
                               String description) {
        super(name + description);
        this.code = code;
        this.status = status;
        this.name = name;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Error info in JSON
     *
     * @return a string in JSON describe the exception
     */
    public ErrorInfo getInfo() {
        return new ErrorInfo();
    }

    public class ErrorInfo {

        public int getCode() {
            return code;
        }

        public int getStatus() {
            return status.value();
        }

        public String getError() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }

    }

}

