package com.sixestates.crawler.parser.dfscallback.handler;

import java.util.List;

import com.sixestates.crawler.model.link.LinkData;

public class DiscardLink implements HandleLinks {

	@Override
	public void handleLinks(List<LinkData> links) {
		// XXX just ignore
	}

	@Override
	public void commit(LinkData link) {
		// XXX ignore
	}

}
