package com.sixestates.crawler.infoprovider.model;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * Created by Zhenchang Liu on 9/3/17.
 */
public class Page {
    
    private Long id;
    private String url;
    private String content;
    private Boolean success;
    
    @JSONField(name = "status_code")
    private Integer statusCode;
    
    @JSONField(name = "final_url")
    private String finalUrl;
    
    @JSONField(name = "time_elapsed")
    private Integer timeElapsed;
    
    @JSONField(name = "fail_reason")
    private String failReason;
    
    public Long getId() {
        
        return id;
    }
    
    public void setId(Long id) {
        
        this.id = id;
    }
    
    public String getUrl() {
        
        return url;
    }
    
    public void setUrl(String url) {
        
        this.url = url;
    }
    
    public String getContent() {
        
        return content;
    }
    
    public void setContent(String content) {
        
        this.content = content;
    }
    
    public Boolean getSuccess() {
        
        return success;
    }
    
    public void setSuccess(Boolean success) {
        
        this.success = success;
    }
    
    public Integer getStatusCode() {
        
        return statusCode;
    }
    
    public void setStatusCode(Integer statusCode) {
        
        this.statusCode = statusCode;
    }
    
    public String getFinalUrl() {
        
        return finalUrl;
    }
    
    public void setFinalUrl(String finalUrl) {
        
        this.finalUrl = finalUrl;
    }
    
    public Integer getTimeElapsed() {
        
        return timeElapsed;
    }
    
    public void setTimeElapsed(Integer timeElapsed) {
        
        this.timeElapsed = timeElapsed;
    }
    
    public String getFailReason() {
        
        return failReason;
    }
    
    public void setFailReason(String failReason) {
        
        this.failReason = failReason;
    }
}
