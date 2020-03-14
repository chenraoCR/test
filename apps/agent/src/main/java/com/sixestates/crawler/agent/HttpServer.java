package com.sixestates.crawler.agent;

import java.net.URL;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler.Context;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.GenericWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.ResourceUtils;

/**
 * start a embedded  web server
 * 
 * @author zhufb
 *
 */
public class HttpServer {


	
	 protected static Logger log = LoggerFactory.getLogger(HttpServer.class.getName());
	
	/**
	 * start a embed server to provide some services
	 * @throws Exception 
	 */
	public static void start(int port) throws Exception{
		try {
			log.info("starting http server");
            Server server = new Server(port);
           
            // create servlet context
            ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
            server.setHandler(root);
            root.setContextPath("/");
            URL url = null;
            
//            InputStream resourceStream = ApplicationResourceUtils.getResourceStream("webapp");
            String resourceUrl = ApplicationResourceUtils.getResourceUrl("conf");
            if(FileUtils.exist(resourceUrl)){
            	url = new URL(ResourceUtils.URL_PROTOCOL_FILE+":"+resourceUrl);
            }else{
            	url = ResourceUtils.getURL("classpath:conf/");
            }
            
            root.setBaseResource(Resource.newResource(url));
            Context servletContext = root.getServletContext();
            
            // create servlet
            DispatcherServlet dispatchServlet =  new DispatcherServlet();
            dispatchServlet.setContextConfigLocation("servlet-context.xml");
            
            // use application context as the parent context of current Web Application Context,
            // so we can use same beanfactory.
            GenericWebApplicationContext webContext = new GenericWebApplicationContext();
//            webContext.setParent(applicationContext);
            webContext.refresh();
            servletContext.setAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, webContext);
            root.addServlet(new ServletHolder(dispatchServlet), "/");
            
            server.start();
            if(AbstractLifeCycle.FAILED.equals(server.getState())){
            	throw new RuntimeException("crawl server start failed !");
            }
            log.info("started http server on port {}",port);
        } catch (Exception e) {
        	log.error("Error when start http server", e);
        	throw e;
        }
	}


    public static void main(String[] args)
    {
        HttpServer server = new HttpServer();
        try {
            server.start(19881);
        }
        catch (Exception e)
        {

        }
    }
}
