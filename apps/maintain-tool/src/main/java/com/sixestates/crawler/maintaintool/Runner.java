package com.sixestates.crawler.maintaintool;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.framework.fdfs.FdfsClient;
import com.framework.fdfs.FdfsClientFactory;
import com.framework.fdfs.FdfsException;
import com.lakeside.core.utils.GZipUtil;
import com.sixestates.crawler.config.BaseConfig;
import com.sixestates.crawler.config.CommonConfig;
import com.sixestates.crawler.maintaintool.dummy.link.DummyLinkData;
import com.sixestates.crawler.maintaintool.dummy.link.DummyLinkDataDAO;
import com.sixestates.crawler.maintaintool.dummy.link.dao.DummyLinkDataDAOImpl;
import com.sixestates.crawler.model.crawlurl.CrawlUrl;
import com.sixestates.crawler.model.crawlurl.CrawlUrlDAO;
import com.sixestates.crawler.model.crawlurl.impl.CrawlUrlDAOImplMySQL;
import com.sixestates.crawler.model.uploadpack.UploadPack;
import com.sixestates.crawler.model.uploadpack.UploadPackDAO;
import com.sixestates.crawler.model.uploadpack.impl.UploadPackDAOImplMySQL;
import com.sixestates.crawler.parser.page.Page;
import com.sixestates.crawler.parser.page.PageException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Runner {
	private static final Logger log = LoggerFactory.getLogger(Runner.class);

	/*public static void main(String argv[]) throws IOException {
		if (!BaseConfig.initConfig()) {
			System.exit(1);
		}


		FdfsClient fclient = FdfsClientFactory.get(CommonConfig.getFdfs_Cluster_Name());

		String path ="group01/M00/20/71/rBJthlvkB4qAJOqlAAWs_Xee-jw8476.gz";
		byte[] rawFile = fclient.download(path);
		String text = null;
		try {
			text = GZipUtil.uncompress(rawFile, "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
		JSONArray jsonArray = JSON.parseArray(text);
		CrawlUrlDAO crawlUrlDAO = new CrawlUrlDAOImplMySQL();

		for (Object object : jsonArray) {
			JSONObject jsonObject = (JSONObject)object;
			long crawurlId = jsonObject.getLong("id");
			CrawlUrl crawlUrl = crawlUrlDAO.query(crawurlId);
			System.out.println(jsonObject.get("id"));
			jsonObject.put("id",null);
			jsonObject.put("external_id",184);
			jsonObject.put("page_type", crawlUrl.getPageType());
			jsonObject.put("parser", crawlUrl.getParser());
			jsonObject.put("source", crawlUrl.getDataSource());

		}

		*//*FileUtils.writeStringToFile(new File("output.txt"), JSON.toJSONString(jsonArray));*//*
		System.out.println(JSON.toJSONString(jsonArray));

		byte[] datafile = GZipUtil.compress(JSON.toJSONString(jsonArray));

		*//*HttpPost httpPost = new HttpPost("http://172.18.109.133:9999/cluster/task/submit");*//*
		HttpPost httpPost = new HttpPost("http://172.18.109.137:9999/cluster/task/submit");
		MultipartEntityBuilder builder = MultipartEntityBuilder.create();
		String filename = "file_examples.gz";
		String md5 = null;
		md5 = DigestUtils.md5Hex(datafile);
		builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
		builder.addPart("file", new ByteArrayBody(datafile, filename));
		builder.addPart("token", new StringBody("123456", ContentType.create(
				"text/plain", Consts.UTF_8)));
		builder.addPart("md5_checksum", new StringBody(md5, ContentType.create(
				"text/plain", Consts.UTF_8)));
		HttpEntity multipart = builder.build();
		httpPost.setEntity(multipart);
		CloseableHttpResponse response = null;
		try {
			response = HttpClients.createDefault().execute(httpPost);
		} catch (IOException e) {
			e.printStackTrace();
		}
		text = EntityUtils.toString(response.getEntity());
		System.out.println(text);

	}*/

	public static void main(String argv[]) {
        if (!BaseConfig.initConfig()) {
			System.exit(1);
		}

		UploadPackDAO uploadPackDAO = new UploadPackDAOImplMySQL();
        CrawlUrlDAO crawlUrlDAO = new CrawlUrlDAOImplMySQL();
		DummyLinkDataDAO dummyLinkDataDAO = new DummyLinkDataDAOImpl();
		FdfsClient fclient = FdfsClientFactory.get(CommonConfig.getFdfs_Cluster_Name());

		long start = 452887;
        int size = 100;
        ExecutorService pool = Executors.newFixedThreadPool(100);
        while (true) {
        	List<UploadPack> uploadPacks = uploadPackDAO.query(start, size);
        	if (uploadPacks.isEmpty()) {
        		break;
			}
        	CountDownLatch doneSignal = new CountDownLatch(uploadPacks.size());
			for (UploadPack uploadPack : uploadPacks) {
				pool.execute(new Runnable() {
					@Override
					public void run() {
						try {
							String path ="group01/M00/20/71/rBJthlvkB4qAJOqlAAWs_Xee-jw8476.gz";
							byte[] rawFile = fclient.download(uploadPack.getPath());
							String text = GZipUtil.uncompress(rawFile, "UTF-8");
							JSONArray jsonArray = JSON.parseArray(text);
							for (Object object : jsonArray) {
								JSONObject jsonObject = (JSONObject)object;
								long crawlURLID = jsonObject.getLong("id");
								CrawlUrl crawlUrl = crawlUrlDAO.query(crawlURLID);
								String key = crawlUrl.getKey().key;
								DummyLinkData dummyLinkData = new DummyLinkData();
								dummyLinkData.setKey(key);
								dummyLinkData.setRawPage(JSON.toJSONString(jsonObject));
								dummyLinkDataDAO.insert(dummyLinkData);
								log.info("key: {}, uploadpack_id: {}", key, uploadPack.getId());
							}

						} catch (FdfsException e) {
							log.error("FDFS raise some exception.", e);
						} catch (IOException e) {
							log.error("decompress gzip file failed.", e);
						} catch (ClassCastException e) {
							log.error("invalid structure, cast to JSONObject failed");
						} finally {
							doneSignal.countDown();
						}
					}
				});
			}
			try {
				doneSignal.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
			
			if (uploadPacks.size() != size) {
        		break;
			} else {
        		start += size;
			}
		}
        pool.shutdown();
        log.info("all task done!");
	}


	public static List<Page> convert(JSONArray jsonArray, Date downloadTime) {
		List<Page> pages = new LinkedList<>();
		for (Object obj : jsonArray) {
			if (obj == null) {
				log.error("null object found when iterating jsonArray");
				continue;
			}
			try {
				JSONObject jsonObj = (JSONObject) obj;
				Page page = new Page(jsonObj, downloadTime);
				pages.add(page);
			} catch (ClassCastException e) {
				log.error("invalid structure, cast to JSONObject failed");
			} catch (PageException e) {
				log.error("construct page failed");
			}
		}
		return pages;
	}
}
