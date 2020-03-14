package com.sixestates.crawler.infoprovider.mvc.dataflow;

import com.sixestates.crawler.datastorage.mysql.datasaver.DataErrorInfo;
import com.sixestates.crawler.datastorage.mysql.datasaver.DataSaver;
import com.sixestates.crawler.infoprovider.model.Page;
import com.sixestates.crawler.infoprovider.mvc.BaseController;
import com.sixestates.crawler.model.crawlurl.CrawlUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("info")
public class DataFlowController extends BaseController {

    private static final Logger logger = LoggerFactory.getLogger(DataFlowController.class);

    @RequestMapping(value = "/crawl-url", method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    Response<List<CrawlUrl>> queryCrawlURL(@RequestBody CrawlDataRequest body) {
        return encapsulate(DataFlowService.getCrawlURL(body.batchId, body.url));
    }

    @RequestMapping(value = "/crawl-webpage", method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    Response<Page> queryCrawlWebpage(@RequestParam Long crawlURLId) {
        return encapsulate(DataFlowService.getCrawlWebpage(crawlURLId));
    }

    @RequestMapping(value = "/data-saver", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    Response<List<Map<String, Object>>> queryInternalDataSaver(@RequestParam long batchId, @RequestParam long crawlURLId,
                                                         @RequestParam (value = "extra", required = false) String extra) {
        return encapsulate(DataFlowService.getInternalDataSaver(batchId, crawlURLId, extra));
    }

    @RequestMapping(value = "/data-error", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    Response<List<DataErrorInfo>> getDataError(@RequestParam long startTime, @RequestParam long endTime) {
        return encapsulate(DataFlowService.getDataError(startTime, endTime));
    }

    @RequestMapping(value = "/data-ext-error", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    Response<List<DataErrorInfo>> getDataErrorExternal(@RequestParam long startTime, @RequestParam long endTime) {
        return encapsulate(DataFlowService.getDataErrorExternal(startTime, endTime));
    }


    @RequestMapping(value = "/data-saver-reverse", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    Response<DataSaver> queryInternalDataSaver(@RequestParam long batchId, @RequestParam long dataId) {
        return encapsulate(DataFlowService.getInternalDataSaver(batchId, dataId));
    }

    @RequestMapping(value = "/data-saver-content", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    Response<Map<String, Map<String, Object>>> queryInternalDataSaverContent(@RequestParam long batchId, @RequestParam long dataId) {
        return encapsulate(DataFlowService.getInternalDataSaverContent(batchId, dataId));
    }

    @RequestMapping(value = "/crawl-url", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    Response<CrawlUrl> queryCrawlURL(@RequestParam Long crawlURLId) {
        return encapsulate(DataFlowService.getCrawlURL(crawlURLId));
    }

    static class CrawlDataRequest {
        public long batchId;
        public String url;
    }

}