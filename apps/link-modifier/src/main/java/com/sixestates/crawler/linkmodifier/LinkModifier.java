package com.sixestates.crawler.linkmodifier;

import com.sixestates.crawler.config.BaseConfig;
import com.sixestates.crawler.model.cache.CacheLoader;

public class LinkModifier {
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

		new LinkHandler().run();
	}
}
