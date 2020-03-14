package com.sixestates.crawler.infoprovider.mvc;

/**
 * Created by @maximin.
 */
public class BaseController {

    protected  <T> Response<T> encapsulate(T data) {
       Response response = new Response();
       response.data = data;
       return response;
    }

    protected static class Response<T> {
        public T data;
    }
}
