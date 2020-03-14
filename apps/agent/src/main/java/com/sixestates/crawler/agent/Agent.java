package com.sixestates.crawler.agent;

import com.sixestates.crawler.config.BaseConfig;
import com.sixestates.crawler.model.cache.CacheLoader;

/**
 * @author zhufb
 *
 */
public class Agent {

	public static void main(String[] args) {
		if (BaseConfig.initConfig()) {
			start();
		} else {
			System.exit(1);
		}
	}

	private static void start() {
		Thread cacheThread = new Thread(new CacheLoader());
		cacheThread.setDaemon(true);
		cacheThread.start();

		int port = Config.getCluster_Module_Port();
		try {
			HttpServer.start(port);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
