package com.sixestates.crawler.parser.dfscallback.handler;

import java.util.LinkedList;
import java.util.List;

import com.sixestates.crawler.model.link.LinkData;
import com.sixestates.crawler.model.link.LinkPack;
import com.sixestates.crawler.model.queue.LinkQueue;

public class Send2Redis implements HandleLinks {
	private List<LinkData> cachedLinks = new LinkedList<>();

	@Override
	public void handleLinks(List<LinkData> links) {
		cachedLinks.addAll(links);
	}

	@Override
	public void commit(LinkData link) {
		LinkPack pack = new LinkPack(cachedLinks, link);
		LinkQueue.get().push(pack);
	}
}
