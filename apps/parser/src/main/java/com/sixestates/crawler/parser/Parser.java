package com.sixestates.crawler.parser;

import com.sixestates.crawler.config.BaseConfig;
import com.sixestates.crawler.model.cache.CacheLoader;
import com.sixestates.crawler.parser.job.TaskParseJob;
import com.sixestates.crawler.parser.script.TemplateLoader;
import com.sixestates.crawler.util.UIDThreadFactory;

/**
 * @author zhufb
 *
 */
public class Parser {
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

		Thread tplThread = new Thread(new TemplateLoader());
		tplThread.setDaemon(true);
		tplThread.start();

		UIDThreadFactory factory = new UIDThreadFactory("task-parse-job");
		int max = Config.getCluster_Task_Parse_Threads();
		for (int i = 0; i < max; i++) {
			factory.newThread(new TaskParseJob()).start();
		}
	}
}
