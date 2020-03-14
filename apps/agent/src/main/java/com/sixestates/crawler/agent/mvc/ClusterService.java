package com.sixestates.crawler.agent.mvc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import com.sixestates.crawler.datastorage.mysql.datasaver.DataSaverErrDAO;
import com.sixestates.crawler.datastorage.mysql.user.User;
import com.sixestates.crawler.datastorage.mysql.user.UserDAO;
import com.sixestates.crawler.datastorage.mysql.user.impl.UserDAOImplMySQL;
import com.sixestates.crawler.datastorage.mysql.userbatch.UserBatch;
import com.sixestates.crawler.datastorage.mysql.userbatch.UserBatchDAO;
import com.sixestates.crawler.datastorage.mysql.userbatch.impl.UserBatchDAOImplMySQL;
import com.sixestates.crawler.model.external.batch.ExternalBatch;
import com.sixestates.crawler.model.external.batch.dao.ExternalBatchDAO;
import com.sixestates.crawler.model.external.batch.dao.impl.ExternalBatchDAOImplMySQL;
import com.sixestates.crawler.model.external.datasaver.dao.ExternalDataSaverDAO;
import com.sixestates.crawler.model.external.datasaver.dao.impl.ExternalDataSaverDAOImplMySQL;
import com.sixestates.crawler.model.external.datasaver.dao.impl.ExternalDataSaverErrDAOImpl;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.framework.fdfs.FdfsClient;
import com.framework.fdfs.FdfsClientFactory;
import com.framework.fdfs.FdfsException;
import com.google.common.collect.Maps;
import com.lakeside.core.utils.time.StopWatch;
import com.sixestates.crawler.config.CommonConfig;
import com.sixestates.crawler.model.agenthost.AgentHostDAO;
import com.sixestates.crawler.model.agenthost.impl.AgentHostDAOImplMySQL;
import com.sixestates.crawler.model.cache.DataSourceCache;
import com.sixestates.crawler.model.cache.DefineCache;
import com.sixestates.crawler.model.cache.LinkRuleCache;
import com.sixestates.crawler.model.crawlurl.CrawlUrl;
import com.sixestates.crawler.model.crawlurl.CrawlUrlDAO;
import com.sixestates.crawler.model.crawlurl.impl.CrawlUrlDAOImplMySQL;
import com.sixestates.crawler.model.define.Define;
import com.sixestates.crawler.model.downloadpack.DownloadPack;
import com.sixestates.crawler.model.downloadpack.DownloadPackDAO;
import com.sixestates.crawler.model.downloadpack.impl.DownloadPackDAOImplMySQL;
import com.sixestates.crawler.model.link.LinkData;
import com.sixestates.crawler.model.link.LinkData.QueueItem;
import com.sixestates.crawler.model.link.dao.LinkDataDAO;
import com.sixestates.crawler.model.link.dao.LinkDataDAOFactory;
import com.sixestates.crawler.model.linkrule.LinkRule;
import com.sixestates.crawler.model.queue.LinkKeyQueue;
import com.sixestates.crawler.model.queue.TaskQueue;
import com.sixestates.crawler.model.resource.Resource;
import com.sixestates.crawler.model.resource.ResourceFactory;
import com.sixestates.crawler.model.uploadpack.UploadPackDAO;
import com.sixestates.crawler.model.uploadpack.impl.UploadPackDAOImplMySQL;
import com.sixestates.crawler.model.useragent.UserAgent;
import com.sixestates.crawler.model.useragent.UserAgentDAO;
import com.sixestates.crawler.model.useragent.impl.UserAgentDAOImplMySQL;
import com.sixestates.crawler.proxy.ProxyPool;

/**
 * cluster service
 * 
 * @author zhufb
 */
public class ClusterService {
	private static final Logger logger = LoggerFactory.getLogger(ClusterService.class.getName());

	private static final ExternalBatchDAO externalBatchDao = new ExternalBatchDAOImplMySQL();
	private static final ExternalDataSaverDAO externalDataSaverDao = new ExternalDataSaverDAOImplMySQL();
	private static final DataSaverErrDAO externalDataSaverErrDao = new ExternalDataSaverErrDAOImpl();
	private static final UserBatchDAO userBatchDao = new UserBatchDAOImplMySQL();
	private static final UserDAO userDao = new UserDAOImplMySQL();
	private static final AgentHostDAO hostDao = new AgentHostDAOImplMySQL();
	private static final DownloadPackDAO downloadPackDao = new DownloadPackDAOImplMySQL();
	private static final CrawlUrlDAO crawlUrlDao = new CrawlUrlDAOImplMySQL();
	private static final UploadPackDAO uploadDao = new UploadPackDAOImplMySQL();
	private static final LinkDataDAO linkDao = LinkDataDAOFactory.getDAO();
	private static final UserAgentDAO userAgentDao = new UserAgentDAOImplMySQL();

	private static final FdfsClient fclient = FdfsClientFactory.get(CommonConfig.getFdfs_Cluster_Name());

	public static Map<String, Object> registerExternalBatch(long token) {
		Map<String, Object> results = new HashMap<>();
		// init a batch
		long batchId = externalBatchDao.init(token);
		if (batchId == 0) {
			results.put("status", "ERR");
			results.put("mesg", "ERR_BATCH_INIT");
			return results;
		}
		results.put("batch_id", batchId);
		// create external_data_saver table
		int ret_code = externalDataSaverDao.create(batchId);
		if (ret_code != 0) {
			results.put("status", "ERR");
			results.put("mesg", "ERR_DATA_SAVER_CREATE");
			return results;
		}
		ret_code = externalDataSaverErrDao.initTableByBatchId(batchId);
		if (ret_code != 0) {
			results.put("status", "ERR");
			results.put("mesg", "ERR_DATA_SAVER_ERR_CREATE");
			return results;
		}

		// register user-batch
		Optional<User> optAdminUser = userDao.getAdminUser();
		if (optAdminUser.isPresent()) {
			userBatchDao.initRecord(optAdminUser.get().getId(), batchId, UserBatch.Type.EXTERNAL);
		} else {
			logger.error("no valid admin user but doesn't matter.");
		}
		// run a batch
		ret_code = externalBatchDao.run(batchId);
		if (ret_code == 0) {
			results.put("status", "ERR");
			results.put("mesg", "ERR_BATCH_RUN");
			return results;
		}
		// good end
		results.put("status", "SUCCESS");
		return results;
	}

	public static Map<String, Object> getExternalBatch(long id) {
		Map<String, Object> results = new HashMap<>();
		try {
			ExternalBatch externalBatch = externalBatchDao.status(id);
			if (!Optional.ofNullable(externalBatch).isPresent()) {
				logger.warn("no such external batch: {}", id);
				results.put("status", "ERR");
				results.put("mesg", "ERR_BATCH_NOT_EXIST");
			} else {
				results.put("status", "SUCCESS");
				Map<String, Object> batch = new HashMap<>();
				batch.put("batch_id", externalBatch.getId());
				batch.put("status", externalBatch.getStatus().str);
				batch.put("count", externalBatch.getCount());
				results.put("batch", batch);
			}
		} catch(Exception e) {
			logger.warn("unexpected exception: {}", e);
			results.put("status", "ERR");
			results.put("mesg", "ERR_BATCH_STATUS");
		}
		return results;
	}

	public static Map<String, Object> terminateExternalBatch(long token, long id) {
		Map<String, Object> results = new HashMap<>();
		int ret_code = externalBatchDao.done(token, id);
		if (ret_code == 0) {
			results.put("status", "ERR");
			results.put("mesg", "ERR_BATCH_TERMINATE");
			return results;
		}
		results.put("status", "SUCCESS");
		return results;
	}

	public static long register(String identity, int cpuCore, double ram, String version) {
		logger.debug("fast-agent: {}, version: {} register...", identity, version);
		long lToken = hostDao.register(identity, cpuCore, ram, version);
//		List<CrawlUrl> listCrawlUrl = crawlUrlDao.queryCrashUrl(lToken);
//		for (CrawlUrl crawlUrlEx: listCrawlUrl) {
//			LinkData linkDataEx = linkDao.get(crawlUrlEx.getKey());
//			LinkKeyQueue.get().push(linkDataEx);
//		}
//		int iCount = crawlUrlDao.updateCrashUrl(listCrawlUrl);
//		logger.debug("fast-agent: {}, re-push {} links", identity, iCount);
		return lToken;
	}

	public static Map<String, Object> getTask(long lToken, String[] includes, String[] excludes) {
		Map<String, Object> result = Maps.newHashMap();

		// check valid fast-agent
		if (!hostDao.exists(lToken)) {
			result.put("status", "NONE");
			logger.warn("invalid fast-agent, token: {}", lToken);
			return result;
		}

		// heartbeat
		hostDao.last_heartbeat(lToken);

		// fetch tasks from LinkKeyQueue
		Queue<LinkData> links = packUrl(includes, excludes);

		// check tasks
		if (links.isEmpty()) {
			result.put("status", "NONE");
			logger.warn("no task for fast-agent, token: {}", lToken);
			return result;
		}
		logger.info("ready to give {} tasks to fast-agent: {}", links.size(), lToken);

		List<Map<String, Object>> queries = new LinkedList<>();
		List<CrawlUrl> crawlUrls = new LinkedList<>();

		while (!links.isEmpty()) {
			LinkData link = links.poll();
			try {
				// retry handle
				if (!(Optional.ofNullable(link.getCookieType()).isPresent()) && link.getRetried() > 0 && crawlUrlDao.isHostTaken(lToken, link)) {
					logger.info("retried task - key: {}, fast-agent: {}", link.toQueueItemStr(), lToken);
					int alive_fast_agent = hostDao.count_alive();
					if (link.getRetried() == alive_fast_agent) {
						link.setStatus(LinkData.Status.CrawlFailed);
						linkDao.set(link);
						LinkKeyQueue.get().rmProcessing(link.toQueueItemStr());
					} else {
						// retry times <= current max fast-agent, re-push
						LinkKeyQueue.get().push(link);
						LinkKeyQueue.get().rmProcessing(link.toQueueItemStr());
						linkDao.setRedisTime(link);
					}
					continue;
				}

				// options
				// cookie
				Map<String, Object> options = new HashMap<>();
				try {
					packCookie(options, link, lToken);
				} catch (NoCookie e) {
					LinkKeyQueue.get().push(link);
					LinkKeyQueue.get().rmProcessing(link.toQueueItemStr());
					linkDao.setRedisTime(link);
					continue;
				}

				// proxy
				Define define = DefineCache.get(link.getDefineId());
				if (define.isUseProxy()) {
					String proxy_addr = ProxyPool.getProxy(link.getDataSource());
					if (proxy_addr.isEmpty()) {
						LinkKeyQueue.get().push(link);
						LinkKeyQueue.get().rmProcessing(link.toQueueItemStr());
						linkDao.setRedisTime(link);
						continue;
					} else {
						options.put("proxy", proxy_addr);
					}
				}

				// headers, link rule
				Map<String, Object> headers = new HashMap<>();
				LinkRule rule = LinkRuleCache.get(link.getUrl());
				if (Optional.ofNullable(rule).isPresent()) {
					headers.putAll(rule.getHeaderMap());
					put(options, "diff_interval", rule.getDiffInterval());
					put(options, "diff_method", rule.getDiffMethod());
					put(options, "diff_parameter", rule.getDiffParamJson());
				}
				if (Optional.ofNullable(link.getHeader()).isPresent()) {
					headers.putAll(link.getHeader());
				}
				if (Optional.ofNullable(link.getUserAgent()).isPresent() && !link.getUserAgent().trim().isEmpty()) {
					headers.put("User-Agent", link.getUserAgent().trim());
				}
				options.put("headers", headers);

				// payload
				if (Optional.ofNullable(link.getPayload()).isPresent() && !link.getPayload().isEmpty()) {
					options.put("payload", link.getPayload());
				}

				// params
				if (Optional.ofNullable(link.getParams()).isPresent() && !link.getParams().isEmpty()) {
					options.put("params", link.getParams());
				}

				// other options
				put(options, "downloader", link.getDownloader());
				put(options, "cookie_type", link.getCookieType());
				put(options, "source", link.getDataSource());
				put(options, "threads", DataSourceCache.get(link.getDataSource()).getThreads());
				put(options, "cooldown", DataSourceCache.get(link.getDataSource()).getCooldown());

				// transfer to CrawlUrl object
				crawlUrls.add(new CrawlUrl(link, options, lToken));
				Map<String, Object> cb_query = new HashMap<>();
				cb_query.put("url", link.getUrl());
				cb_query.put("options", options);
				cb_query.put("key", link.toQueueItemStr());
                queries.add(cb_query);
			} catch (Exception e) {
				logger.error("processing task error, batch: {}, url: {}, key: {}", link.getBatchId(), link.getUrl(), link.key(), e);
				LinkKeyQueue.get().push(link);
				LinkKeyQueue.get().rmProcessing(link.toQueueItemStr());
				linkDao.setRedisTime(link);
			}
		}

		if (queries.isEmpty()) {
			result.put("status", "NONE");
			logger.warn("no available task for this fast-agent: {}", lToken);
			return result;
		}

		// register a download pack id
		DownloadPack downloadPack = downloadPackDao.insert(lToken);

		// insert crawlUrls into crawlUrl table
		Map<String, CrawlUrl> keyToCrawlUrl = crawlUrlDao.download(downloadPack, crawlUrls);

		// check crawlUrl insert successful
		Iterator<Map<String, Object>> iterator = queries.iterator();
		while (iterator.hasNext()) {
			Map<String, Object> cb_query = iterator.next();
			String cb_query_key = MapUtils.getString(cb_query, "key");
			if (Optional.ofNullable(cb_query_key).isPresent()) {
			    CrawlUrl crawlUrl = keyToCrawlUrl.get(cb_query_key);
			    if (Optional.ofNullable(crawlUrl).isPresent()) {
			        cb_query.put("id", crawlUrl.getId());
			        LinkKeyQueue.get().rmProcessing(cb_query_key);
                } else {
			        iterator.remove();
			        LinkData linkData = linkDao.get(LinkData.unpackQueueItemStr(cb_query_key));
			        LinkKeyQueue.get().push(linkData);
			        LinkKeyQueue.get().rmProcessing(cb_query_key);
                    logger.error("insert into sc_crawl_url failed, key: {}", cb_query_key);
                }
            }
		}

		logger.info ("fast-agent: {} actually get {} tasks.", lToken, queries.size());

		result.put("status", "OK");
		result.put("queries", queries);
		return result;
	}

	private static class NoCookie extends Exception {
		private static final long serialVersionUID = 1L;
	}

	private static void packCookie(Map<String, Object> options, LinkData link, long token) throws NoCookie {
		if (link.getCookieType() == null) {
			return;
		}
		// use source's cookie, return null
		if (link.getCookieType() == LinkData.CookieType.source) {
			ResourceFactory r = ResourceFactory.get(link.getDataSource());
			Resource resource = r.getResourceAddCount(token);
			if (resource == null) {
				throw new NoCookie();
			}
			put(options, "cookie", resource.getResource());
			put(options, "resource_id", resource.getId());
			return;
		}
		// use current cookie
		if (link.getCookieType() == LinkData.CookieType.current) {
			put(options, "cookie", link.getCookie());
			return;
		}
		// else use the family resource
		// XXX 修改后其实这部分已经没法工作了
		Resource r = ResourceFactory.getFamilyResource(link.getDataSource(), link.getCookieType().str);
		if (r == null) {
			logger.error("Did not get family available resource,source:{}, family: {}", link.getDataSource(),
					link.getCookieType());
			return;
		}
		put(options, "cookie", r.getResource());
	}

	private static Queue<LinkData> packUrl(String[] includes, String[] excludes) {
		StopWatch sw = new StopWatch();
		sw.start();
		// get item from LinkKeyQueue
		List<QueueItem> queueItems = LinkKeyQueue.get().pop(includes, excludes);
		logger.info("load {} items from LinkKeyQueue, elapsed time: {} ms", queueItems.size(), sw.getElapsedTime());

		sw.stop();

		Queue<LinkData> links = new LinkedList<>();

		if (queueItems.isEmpty()) {
			return links;
		}

		Set<QueueItem> queueItemsSet = new HashSet<>(queueItems);

		for (QueueItem queueItem: queueItemsSet) {
			// get LinkData from storage, i.e. MySQL
			LinkData link = linkDao.getThenCacheResult(queueItem);
			if (Optional.ofNullable(link).isPresent()) {
				if (LinkData.Status.Init == link.getStatus()) {
					links.add(link);
				} else {
					logger.warn("link - {} is not in init status", queueItem.key);
					LinkKeyQueue.get().rmProcessing(link.toQueueItemStr());
				}
			} else {
				logger.warn("invalid link - {}", queueItem.key);
			}
		}

		return links;
	}

	public static long submitTask(long token, List<Long> queryIds, String md5, byte[] bytes) {
		String path = null;
		for (int i = 0; i < 3; i++) {
			try {
				logger.info("uploading file to fdfs");
				path = fclient.upload(bytes, "gz");
				logger.info("store file to fdfs succeed");
				break;
			} catch (FdfsException e) {
				if (i == 2) {
					throw e;
				}
			}
		}
		logger.debug("insert into uploadDao");
		Long uploadId = uploadDao.insert(token, md5, path);
		logger.info("upload_pack id: {}, md5: {}, path: {}", uploadId, md5, path);
		if (queryIds != null) {
			crawlUrlDao.upload(queryIds, uploadId);
		}
		TaskQueue.get().push(new String[] {uploadId.toString()});
		return uploadId;
	}

	public static List<Object> getUserAgents() {
		List<UserAgent> userAgentObjs = userAgentDao.query();
		List<Object> userAgents = new ArrayList<>(userAgentObjs.size());
		for (UserAgent userAgentObj : userAgentObjs) {
			userAgents.add(userAgentObj.getUserAgent());
		}
		return userAgents;
	}

	private static <T> void put(Map<String, T> map, String key, T value) {
		if (value == null) {
			return;
		}
		map.put(key, value);
	}
}