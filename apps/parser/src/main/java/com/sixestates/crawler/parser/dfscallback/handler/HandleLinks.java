package com.sixestates.crawler.parser.dfscallback.handler;

import java.util.List;

import com.sixestates.crawler.model.link.LinkData;

/**
 * 用于处理link, 不同的实现在实际处理的细节上不一样, 但是整体的处理逻辑必须保持一致
 * @author libo
 *
 */
public interface HandleLinks {
	/**
	 * 提交一组link等待处理, 注意这个处理是惰性的处理, 即知道调用commit前, 提交的link都没有真正得到处理
	 * @param links 需要处理的links
	 */
	void handleLinks(List<LinkData> links);

	/**
	 * 对前面通过handleLinks提交的links集中进行处理, 因为业务逻辑上新的link和一个旧的link所关联
	 * 所以这里要求pass一个旧的link
	 * @param link 和当前新生成的link关联的旧的link
	 */
	void commit(LinkData link);
}
