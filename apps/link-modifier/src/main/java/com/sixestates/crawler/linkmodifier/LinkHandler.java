package com.sixestates.crawler.linkmodifier;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;

import com.lakeside.core.utils.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.ThreadUtils;
import com.sixestates.crawler.model.cache.DefineCache;
import com.sixestates.crawler.model.define.Define;
import com.sixestates.crawler.model.link.LinkData;
import com.sixestates.crawler.model.link.LinkPack;
import com.sixestates.crawler.model.link.LinkSnapshot;
import com.sixestates.crawler.model.link.dao.LinkDataDAO;
import com.sixestates.crawler.model.link.dao.LinkDataDAOFactory;
import com.sixestates.crawler.model.link.dao.impl.LinkSnapshotDAOImplNoSQL;
import com.sixestates.crawler.model.link.evaluate.LinkEvaluator;
import com.sixestates.crawler.model.queue.LinkKeyQueue;
import com.sixestates.crawler.model.queue.LinkQueue;

/**
 * 负责将解析产生的新Link消耗掉
 * 
 * 解析生成的新的Link未做更多的处理, 直接放到了redis,
 * 本程序负责从redis popout new link进一步处理(去除重复, 计算优先级)后放入redis以及ssdb
 * */
public class LinkHandler implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(LinkHandler.class);

	private static final int batchSize = Config.getCluster_Task_Link_Batch_Size();
	private static final int threshold = 500;

	private static final Map<String, String> handlingPacks = new HashMap<>();
	private static final LinkDataDAO linkDao = LinkDataDAOFactory.getDAO();

	@Override
	public void run() {
		while (!Thread.interrupted()) {
			List<LinkData> newLinks = new LinkedList<>();
			List<LinkData> oldLinks = new LinkedList<>();
			for (int i = 0; i < batchSize; i++) {
				String packStr = LinkQueue.get().pop();
				if (Optional.ofNullable(packStr).isPresent()) {
					LinkPack.Pack pack = LinkPack.transform(packStr);
					newLinks.addAll(pack.newLinks());
					oldLinks.add(pack.oldLink());
					handlingPacks.put(pack.oldLink().key(), packStr);
				} else {
					break;
				}
				if (newLinks.size() > threshold) {
					break;
				}
			}
			if (oldLinks.isEmpty() && newLinks.isEmpty()) {
				ThreadUtils.wait(1);
			} else {
				// XXX 注意, 必须先处理新生成的link, 再处理旧的link
				logger.info("start to process links, {} newLink, {} oldLinks", newLinks.size(), oldLinks.size());
				handleNewLinks(newLinks);
				handleOldLinks(oldLinks);
			}
		}
	}

	private void handleOldLinks(List<LinkData> links) {
		if (links.isEmpty()) {
			return;
		}

		StopWatch sw = new StopWatch();
		sw.start();

		Map<Long, List<LinkData>> batchIdLinks = new HashMap<>();
		for (LinkData link: links) {
			long batchId = link.getBatchId();
			List<LinkData> oldLinks = batchIdLinks.getOrDefault(batchId, new LinkedList<>());
			oldLinks.add(link);
			batchIdLinks.put(batchId, oldLinks);
		}

		for (long batchId: batchIdLinks.keySet()) {
			List<LinkData> oldLinks = batchIdLinks.get(batchId);
			linkDao.set(oldLinks);
		}

		for (LinkData link: links) {
			LinkQueue.get().rmProcessing(handlingPacks.get(link.key()));
		}

		handlingPacks.clear();
		logger.info("process {} old links done, elapsed time: {} ms", links.size(), sw.getElapsedTime());
		sw.stop();
	}

	private void handleNewLinks(List<LinkData> links) {
		if (links.isEmpty()) {
			return;
		}

		StopWatch sw = new StopWatch();
		sw.start();

		Map<Long, List<LinkData>> batchIdLinks = new HashMap<>();
		for (LinkData link: links) {
			long batchId = link.getBatchId();
			List<LinkData> newLinks = batchIdLinks.getOrDefault(batchId, new LinkedList<>());
			newLinks.add(link);
			batchIdLinks.put(batchId, newLinks);
		}

		logger.info("process into batch map, {} batches, elapsed time: {} ms", batchIdLinks.size(), sw.getElapsedTime());
		sw.reset();

		for (long batchId: batchIdLinks.keySet()) {
			Map<LinkData.QueueItem, LinkData> linksMap = new HashMap<>();
			List<LinkData> newLinks = batchIdLinks.get(batchId);

			for (LinkData newLink : newLinks) {
				linksMap.put(newLink.toQueueItem(), newLink);
			}

			Queue<LinkData> uncheckLinks = new LinkedList<>();
			uncheckLinks.addAll(linksMap.values());
			List<LinkData> checkLinks = new LinkedList<>();

			while (!uncheckLinks.isEmpty()) {
				checkLinks.add(uncheckLinks.poll());
				if (checkLinks.size() == 50) {
					Collection<LinkData.QueueItem> existQueueItems = linkDao.exist(checkLinks);
					for (LinkData.QueueItem queueItem : existQueueItems) {
						linksMap.remove(queueItem);
					}
					checkLinks.clear();
				}
			}
			if (!checkLinks.isEmpty()) {
				Collection<LinkData.QueueItem> existQueueItems = linkDao.exist(checkLinks);
				for (LinkData.QueueItem queueItem : existQueueItems) {
					linksMap.remove(queueItem);
				}
				checkLinks.clear();
			}

			if (linksMap.isEmpty()) {
				logger.info("batch: {} process done, {} new links, elapsed time: {} ms", batchId, 0, sw.getElapsedTime());
				sw.reset();
				continue;
			}

			Map<String, LinkSnapshot> snapshotMap = LinkSnapshotDAOImplNoSQL.get().get(linksMap.values());
			List<LinkData> queueLinks = new LinkedList<>();
			List<LinkData> cacheLinks = new LinkedList<>();

			for (Entry<LinkData.QueueItem, LinkData> entry : linksMap.entrySet()) {
				LinkData newLink = entry.getValue();
				LinkSnapshot snapshot = snapshotMap.get(newLink.key());
				Define define = DefineCache.get(newLink.getDefineId());
				LinkEvaluator.setupLinkProp(define, newLink, snapshot);

				if (newLink.getPriority() > 0) {
					queueLinks.add(newLink);
				} else {
					newLink.setStatus(LinkData.Status.ParseSuccess);
				}

				cacheLinks.add(newLink);
			}
			// insert into MySQL
			linkDao.set(cacheLinks);
			// insert into Redis
			for (LinkData newLink : queueLinks) {
				LinkKeyQueue.get().push(newLink);
				linkDao.setRedisTime(newLink);
			}
			logger.info("batch: {} process done, {} new links, elapsed time: {} ms", batchId, queueLinks.size(), sw.getElapsedTime());
			sw.reset();
		}
		sw.stop();
	}
}
