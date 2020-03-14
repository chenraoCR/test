package com.sixestates.crawler.parser.dfscallback;

import com.sixestates.crawler.datastorage.BaseDao;
import com.sixestates.crawler.datastorage.ClusterStorer;
import com.sixestates.crawler.datastorage.mysql.ExternalMySQLDao;
import com.sixestates.crawler.datastorage.mysql.MySQLDao;
import com.sixestates.crawler.datastorage.mysql.datasaver.DataSaverErrDAO;
import com.sixestates.crawler.datastorage.mysql.datasaver.impl.DataSaverErrDAOImpl;
import com.sixestates.crawler.model.external.datasaver.dao.impl.ExternalDataSaverErrDAOImpl;
import com.sixestates.crawler.model.link.LinkData;
import com.sixestates.crawler.model.link.LinkData.CookieType;
import com.sixestates.crawler.model.link.LinkData.Status;
import com.sixestates.crawler.model.link.LinkData.Strategy;
import com.sixestates.crawler.model.link.LinkSnapshot;
import com.sixestates.crawler.model.link.dao.impl.LinkSnapshotDAOImplNoSQL;
import com.sixestates.crawler.parser.Config;
import com.sixestates.crawler.parser.dataCheck.DataCheckUtil;
import com.sixestates.crawler.parser.dataCheck.DataErrService;
import com.sixestates.crawler.parser.dfscallback.handler.HandleLinks;
import com.sixestates.crawler.parser.page.Page;
import com.sixestates.engine.model.FieldModel;
import com.sixestates.engine.model.LinkItemModel;
import org.apache.commons.lang3.StringUtils;
import sixestates.crawler.parser.Convert2CascadeMap;
import sixestates.crawler.parser.RowHandler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConvertMetaSaveLink extends Convert2CascadeMap {
	private static final String FIELD_LINK_URL = "link_url";
	private static final String FIELD_PAYLOAD = "payload";
	private static final Pattern FIELD_HEADER = Pattern.compile("^header-(.*)");
	private static final String FORBIDDEN_FIELD_HEADER = "header-user-agent";
	private static final String FIELD_USER_AGENT = "user_agent";
	private LinkedList<LinkData> newLinks;
	private boolean saveCategory = true;
	private HandleLinks linkHandler;
	private LinkData currentLink;
	private FieldInsideLinkItemHandler fieldLIHandler = new FieldInsideLinkItemHandler();
	private boolean isURLInLICFieldBuilt = false;

	public ConvertMetaSaveLink(HandleLinks linkHandler, LinkData currentLink, Page page, boolean isExternal) {
		super(new SaveMetaData() {

			@Override
			public void handle(String table, Map<String, Map<String, Object>> row, boolean isExternal) {
				Map<String, Object> crawlInfo = new HashMap<>();
				crawlInfo.put("_url", page.getFinalURL());
				crawlInfo.put("_fet_time", page.getFetchTime());
				crawlInfo.put("_raw_url", page.getRawURL());
				crawlInfo.put("_crawl_url_id", page.getCrawlURLID());
				crawlInfo.put("_batch_id", currentLink.getBatchId());
				row.put(ClusterStorer.OTHER_COLUMN_FAMILY, crawlInfo);
				super.handle(table, row, isExternal);
			}
			
		}, isExternal);
		this.linkHandler = linkHandler;
		this.currentLink = currentLink;
	}

	@Override
	protected void preVisitLink() {
		newLinks = new LinkedList<>();
	}

	@Override
	protected void postVisitLink() {
		if (newLinks.isEmpty()) {
			return;
		}
		String incStr = activeLinkModel().getIncrease();
		if (incStr != null && incStr.equals("true")) {
			LinkData lastLink = newLinks.getLast();
			LinkSnapshot snapshot = LinkSnapshotDAOImplNoSQL.get().get(lastLink.key());
			saveCategory = saveCategory && (snapshot == null);
		}
		String categoryStr = activeLinkModel().getCategory();
		if (categoryStr != null && categoryStr.equals("true")) {
			if (!saveCategory) {
				return;
			}
		}
		linkHandler.handleLinks(newLinks);
	}

	@Override
	protected void handleLinkItem(LinkItemModel model) {
		try {
			@SuppressWarnings("unused")
			URL testURL = new URL(model.url.toString());
		} catch (MalformedURLException e) {
			return;
		}
		if (currentLink.getMaxLayer() > 0 &&
				currentLink.getLayer() + 1 > currentLink.getMaxLayer()) {
			return;
		}
		String parser = activeLinkModel().getParser();
		if (StringUtils.isEmpty(parser)) {
			parser = currentLink.getParser();
		}
		final String capturedParser = parser;

		LinkData newLink = new LinkData(new LinkData.ParamSupply() {
			@Override
			public String url() {
				return model.url.toString();
			}
			@Override
			public Status status() {
				return Status.Init;
			}
			@Override
			public Integer retried() {
				return 0;
			}
			@Override
			public String parser() {
				return capturedParser;
			}
			@Override
			public String parentKey() {
				return currentLink.key();
			}
			@Override
			public String payload() {
				return model.payload;
			}
			@Override
			public Map<String, String> header() {
				return model.header;
			}
			@Override
			public String userAgent() {
				return model.userAgent;
			}
			@Override
			public String params() {
				return model.params;
			}
			@Override
			public String pageType() {
				return activeLinkModel().getPage_type();
			}
			@Override
			public Strategy strategy() {
				return Strategy.convert(activeLinkModel().getStrategy());
			}
			@Override
			public Integer maxRetry() {
				return currentLink.getMaxRetry();
			}
			@Override
			public Integer maxLayer() {
				return currentLink.getMaxLayer();
			}
			@Override
			public Integer layer() {
				return currentLink.getLayer() + 1;
			}
			@Override
			public String extra() {
				return currentLink.getExtra();
			}
			@Override
			public String downloader() {
				return activeLinkModel().getDownloader();
			}
			@Override
			public Long batchId() {
				return currentLink.getBatchId();
			}
			@Override
			public Long defineId() {
				return currentLink.getDefineId();
			}
			@Override
			public String dataSource() {
				return currentLink.getDataSource();
			}
			@Override
			public CookieType cookieType() {
				return CookieType.convert(model.cookie);
			}
			@Override
			public String cookie() {
				if (CookieType.current.str.equals(model.cookie)) {
					return currentLink.getCookie();
				} else {
					return null;
				}
			}
			@Override
			public boolean category() {
				return Boolean.valueOf(activeLinkModel().getCategory());
			}
		});
		newLinks.add(newLink);
	}


	@Override
	protected void handleFieldInsideLinkItem(FieldModel model) {
		String fieldName = activeLICChild();
		if (FIELD_LINK_URL.equals(fieldName)) {
			fieldLIHandler.handleLinkURL(model);
		} else if (FIELD_HEADER.matcher(fieldName).matches() && !FORBIDDEN_FIELD_HEADER.equals(fieldName)) {
			fieldLIHandler.handleHeader(model);
		} else if (FIELD_PAYLOAD.equals(fieldName)) {
			fieldLIHandler.handlePayload(model);
		} else if (FIELD_USER_AGENT.equals(fieldName)) {
			fieldLIHandler.handleUserAgent(model);
		}
	}

	/**
	 * 在template engine中，如果link -> item -> field结构中name = link_url的field出现在name = payload/header/user_agent之后
	 * 则payload/header/user_agent生成的数据不会被添加进该层级产生的LinkData中
	 */
	private class FieldInsideLinkItemHandler {

		private void handlePayload(FieldModel model) {
			if (isURLInLICFieldBuilt) {
				newLinks.getLast().setPayload(model.data.toString());
			}
		}

		private void handleHeader(FieldModel model) {
			if (isURLInLICFieldBuilt) {
			    Matcher matcher = FIELD_HEADER.matcher(model.getName());
			    if (matcher.matches()) {
					String header = matcher.group(1);
					newLinks.getLast().supplementHeader(header, model.data.toString());
				}
			}
		}

		private void handleUserAgent(FieldModel model) {
			if (isURLInLICFieldBuilt) {
				newLinks.getLast().setUserAgent(model.data.toString());
			}
		}

		private void handleLinkURL(FieldModel model) {
			try {
				new URL(model.data.toString());
			} catch (MalformedURLException e) {
				return;
			}
			if (currentLink.getMaxLayer() > 0 &&
					currentLink.getLayer() + 1 > currentLink.getMaxLayer()) {
				return;
			}
			String parser = activeLinkModel().getParser();
			if (StringUtils.isEmpty(parser)) {
				parser = currentLink.getParser();
			}
			final String capturedParser = parser;

			LinkData newLink = new LinkData(new LinkData.ParamSupply() {
				@Override
				public String url() {
					return model.data.toString();
				}

				@Override
				public Strategy strategy() {
					return Strategy.convert(activeLinkModel().getStrategy());
				}

				@Override
				public Status status() {
					return Status.Init;
				}

				@Override
				public Integer retried() {
					return 0;
				}

				@Override
				public String parser() {
					return capturedParser;
				}

				@Override
				public String parentKey() {
					return currentLink.key();
				}

				@Override
				public String params() {
					return activeLinkItemContainerModel().params;
				}

				@Override
				public String payload() {
					return null;
				}

				@Override
				public Map<String, String> header() {
					return null;
				}

				@Override
				public String userAgent() {
					return null;
				}

				@Override
				public String pageType() {
					return activeLinkModel().getPage_type();
				}

				@Override
				public Integer maxRetry() {
					return currentLink.getMaxRetry();
				}

				@Override
				public Integer maxLayer() {
					return currentLink.getMaxLayer();
				}

				@Override
				public Integer layer() {
					return currentLink.getLayer() + 1;
				}

				@Override
				public String extra() {
					return currentLink.getExtra();
				}

				@Override
				public String downloader() {
					return activeLinkModel().getDownloader();
				}

				@Override
				public Long batchId() {
					return currentLink.getBatchId();
				}

				@Override
				public Long defineId() {
					return currentLink.getDefineId();
				}

				@Override
				public String dataSource() {
					return currentLink.getDataSource();
				}

				@Override
				public CookieType cookieType() {
					return CookieType.convert(activeLinkItemContainerModel().cookie);
				}

				@Override
				public String cookie() {
					if (CookieType.current.str.equals(activeLinkItemContainerModel().cookie)) {
						return currentLink.getCookie();
					} else {
						return null;
					}
				}

				@Override
				public boolean category() {
					return Boolean.valueOf(activeLinkModel().getCategory());
				}
			});
			newLinks.add(newLink);
			isURLInLICFieldBuilt = true;
		}

	}

	@Override
	protected void preVisitLinkIC() {
	   	isURLInLICFieldBuilt = false;
	}

	public static class SaveMetaData implements RowHandler {

		@Override
		public void handle(String table, Map<String, Map<String, Object>> row, boolean isExternal) {
			if(!DataErrService.checkData(row, table, isExternal)){
				return;
			}

			List<BaseDao> daos = ClusterStorer.getDao(table, Config.getKafka_Cluster_Name(), isExternal);
			for (BaseDao dao : daos) {
				dao.save(row);
			}
		}

	}
}


