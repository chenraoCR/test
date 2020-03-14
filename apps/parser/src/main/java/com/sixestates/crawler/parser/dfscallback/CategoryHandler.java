package com.sixestates.crawler.parser.dfscallback;

import com.sixestates.crawler.model.link.LinkData;
import com.sixestates.crawler.model.seed.Seed;
import com.sixestates.crawler.model.seed.SeedDAO;
import com.sixestates.crawler.model.seed.impl.SeedDAOImplMySQL;
import com.sixestates.crawler.parser.dfscallback.handler.HandleLinks;
import com.sixestates.engine.model.FieldModel;
import com.sixestates.engine.model.GroupItemModel;
import com.sixestates.engine.model.ItemModel;
import com.sixestates.engine.model.LinkItemModel;
import com.sixestates.engine.model.Model;
import com.sixestates.engine.model.visit.DFSCallback;

import org.apache.commons.lang3.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;

public class CategoryHandler extends DFSCallback {
    private SeedDAO seedDAO = new SeedDAOImplMySQL();

    private static final String SNAPSHOT = "product-snapshot";
    private static final String FIELD_LINK_URL = "link_url";
    private static final String FIELD_PAYLOAD = "payload";
    private static final Pattern FIELD_HEADER = Pattern.compile("^header-(.*)");
    private static final String FORBIDDEN_FIELD_HEADER = "header-user-agent";
    private static final String FIELD_USER_AGENT = "user_agent";
    private FieldInsideLinkItemHandler fieldLIHandler = new FieldInsideLinkItemHandler();
    private boolean isSeedInLICFieldBuilt = false;
    private boolean isLinkInLICFieldBuilt = false;

    private LinkedList<Seed> seeds;
    private LinkedList<LinkData> links;

    private HandleLinks linkHandler;
	private LinkData link;
    
    public CategoryHandler(HandleLinks lh, LinkData link) {
    	linkHandler = lh;
    	this.link = link;
	}

    @Override
    protected void preVisitMeta() {
        // FIXME Assert.error("found meta tag in beautifulme category link");
    }

    @Override
    protected void postVisitMeta() {
    	// FIXME Assert.error("found meta tag in beautifulme category link");
    }

    @Override
    protected void preVisitLink() {
        seeds = new LinkedList<>();
        links = new LinkedList<>();
    }

    @Override
    protected void postVisitLink() {
        seedDAO.insert(seeds);
        linkHandler.handleLinks(links);
    }

    @Override
    protected void preVisitLinkIC() {
        Model urlModel = activeLinkItemContainerModel().get(FIELD_LINK_URL);
        isSeedInLICFieldBuilt = false;
        isLinkInLICFieldBuilt = false;

        if (!Optional.ofNullable(urlModel).isPresent()) {
        	// FIXME Assert.error("field tag inside link tag, but no link_url");
        }
    }

    @Override
    protected void handleGroupItem(GroupItemModel groupItemModel) {
    	// FIXME Assert.error("found GroupItemModel in beautifulme category link");
    }

    @Override
    protected void handleItem(ItemModel model) {
    	// FIXME Assert.error("found ItemModel in beautifulme category link");
    }

    @Override
    protected void handleLinkItem(LinkItemModel model) {
        // validate URL
        try {
            new URL(model.url.toString());
        } catch (MalformedURLException e) {
            return;
        }

        // check layer
        if (this.link.getMaxLayer() > 0 &&
                this.link.getLayer() + 1 > this.link.getMaxLayer()) {
            return;
        }

        String parser = activeLinkModel().getParser();

        if (StringUtils.isEmpty(parser)) {
            parser = this.link.getParser();
        }

        final String final_Parser = parser;

        if (SNAPSHOT.equals(activeLinkModel().getPage_type())) {
            Seed seed = new Seed();
            seed.setCookieType(model.cookie);
            seed.setDownloader(activeLinkModel().getDownloader());
            seed.setExtra("test");
            seed.setId(null);
            seed.setPageType(activeLinkModel().getPage_type());
            seed.setParams(null);
            seed.setParserName(parser);
            seed.setProject_id(56L);
            seed.setSourceName(this.link.getDataSource());
            seed.setStatus(Seed.Status.ENABLED);
            seed.setUrl(model.url.toString());
            seed.setHeader(model.header);
            seed.setPayload(model.payload);
            seeds.add(seed);
        } else {
            LinkData newLink = new LinkData(new LinkData.ParamSupply() {

                @Override
                public String url() {
                    return model.url.toString();
                }

                @Override
                public LinkData.Status status() {
                    return LinkData.Status.Init;
                }

                @Override
                public Integer retried() {
                    return 0;
                }

                @Override
                public String parser() {
                    return final_Parser;
                }

                @Override
                public String parentKey() {
                    return link.key();
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
                public Integer maxRetry() {
                    // TODO Auto-generated method stub
                    return null;
                }

                @Override
                public LinkData.Strategy strategy() {
                    return LinkData.Strategy.convert(activeLinkModel().getStrategy());
                }

                @Override
                public Integer maxLayer() {
                    // TODO Auto-generated method stub
                    return null;
                }

                @Override
                public Integer layer() {
                    return link.getLayer() + 1;
                }

                @Override
                public String extra() {
                    return link.getExtra();
                }

                @Override
                public String downloader() {
                    return activeLinkModel().getDownloader();
                }

                @Override
                public Long batchId() {
                    return link.getBatchId();
                }

                @Override
                public Long defineId() {
                    return link.getDefineId();
                }

                @Override
                public String dataSource() {
                    return link.getDataSource();
                }

                @Override
                public LinkData.CookieType cookieType() {
                    return LinkData.CookieType.convert(model.cookie);
                }

                @Override
                public String cookie() {
                    if (LinkData.CookieType.current.str.equals(model.cookie)) {
                        return link.getCookie();
                    } else {
                        return null;
                    }
                }

                @Override
                public boolean category() {
                    return Boolean.valueOf(activeLinkModel().getCategory());
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
            });
            links.add(newLink);
        }
    }

    @Override
    protected void handleFiledInsideGroup(FieldModel model) {
    	// FIXME Assert.error("found FiledInsideGroup in beautifulme category link");
    }

    @Override
    protected void handleFieldInsideMeta(FieldModel model) {
    	// FIXME Assert.error("found FiledInsideMeta in beautifulme category link");
    }

    @Override
    protected void handleFieldInsideField(FieldModel model) {
    	// FIXME Assert.error("found FiledInsideField in beautifulme category link");
    }

    @Override
    protected void handleFieldInsideItem(FieldModel model) {
    	// FIXME Assert.error("found FiledInsideItem in beautifulme category link");
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

    private class FieldInsideLinkItemHandler {

        private void handlePayload(FieldModel model) {
            String data = model.data.toString();
            if (isSeedInLICFieldBuilt) {
                seeds.getLast().setPayload(data);
            }
            if (isLinkInLICFieldBuilt) {
                links.getLast().setPayload(data);
            }
        }

        private void handleHeader(FieldModel model) {
            String data = model.data.toString();
            String header = FIELD_HEADER.matcher(model.getName()).group(1);
            if (isSeedInLICFieldBuilt) {
                seeds.getLast().supplementHeader(header, data);
            }
            if (isLinkInLICFieldBuilt) {
                links.getLast().supplementHeader(header, data);
            }
        }

        private void handleUserAgent(FieldModel model) {
            String data = model.data.toString();
            if (isLinkInLICFieldBuilt) {
                links.getLast().setUserAgent(data);
            }
        }

        private void handleLinkURL(FieldModel model) {
            // validate URL
            try {
                new URL(model.data.toString());
            } catch (MalformedURLException e) {
                return;
            }
            // check layer
            if (link.getMaxLayer() > 0 && link.getLayer() + 1 > link.getMaxLayer()) {
                return;
            }
            // check parser
            String parser = activeLinkModel().getParser();
            if (StringUtils.isEmpty(parser)) {
                parser = link.getParser();
            }

            final String final_Parser = parser;
            final String url = model.data.toString();

            if (SNAPSHOT.equals(activeLinkModel().getPage_type())) {
                Seed seed = new Seed();
                seed.setCookieType(activeLinkItemContainerModel().cookie);
                seed.setDownloader(activeLinkModel().getDownloader());
                seed.setExtra("test");
                seed.setId(null);
                seed.setPageType(activeLinkModel().getPage_type());
                seed.setParams(null);
                seed.setParserName(parser);
                seed.setProject_id(56L);
                seed.setSourceName(link.getDataSource());
                seed.setStatus(Seed.Status.ENABLED);
                seed.setUrl(url);
                seeds.add(seed);
                isSeedInLICFieldBuilt = true;
            } else {
                LinkData newLink = new LinkData(new LinkData.ParamSupply() {
                    @Override
                    public String url() {
                        return url;
                    }

                    @Override
                    public LinkData.Strategy strategy() {
                        return LinkData.Strategy.convert(activeLinkModel().getStrategy());
                    }

                    @Override
                    public LinkData.Status status() {
                        return LinkData.Status.Init;
                    }

                    @Override
                    public Integer retried() {
                        return 0;
                    }

                    @Override
                    public String parser() {
                        return final_Parser;
                    }

                    @Override
                    public String parentKey() {
                        return link.key();
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
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public Integer maxLayer() {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public Integer layer() {
                        return link.getLayer() + 1;
                    }

                    @Override
                    public String extra() {
                        return link.getExtra();
                    }

                    @Override
                    public String downloader() {
                        return activeLinkModel().getDownloader();
                    }

                    @Override
                    public Long batchId() {
                        return link.getBatchId();
                    }

                    @Override
                    public Long defineId() {
                        return link.getDefineId();
                    }

                    @Override
                    public String dataSource() {
                        return link.getDataSource();
                    }

                    @Override
                    public LinkData.CookieType cookieType() {
                        return LinkData.CookieType.convert(activeLinkItemContainerModel().cookie);
                    }

                    @Override
                    public String cookie() {
                        if (LinkData.CookieType.current.str.equals(activeLinkItemContainerModel().cookie)) {
                            return link.getCookie();
                        } else {
                            return null;
                        }
                    }

                    @Override
                    public boolean category() {
                        return Boolean.valueOf(activeLinkModel().getCategory());
                    }
                });
                links.add(newLink);
                isLinkInLICFieldBuilt = true;
            }
        }

    }

    @Override
    protected void handleFieldInsidePage(FieldModel model) {
    }
}
