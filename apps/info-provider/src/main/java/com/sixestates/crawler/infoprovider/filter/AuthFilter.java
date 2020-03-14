package com.sixestates.crawler.infoprovider.filter;

import com.sixestates.crawler.infoprovider.exception.common.AuthException;
import com.sixestates.crawler.infoprovider.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by @maximin.
 */
public class AuthFilter implements Filter {

    private static final String salt = "2e3817293fc275dbee74bd71ce6eb056";
    private static final Logger logger = LoggerFactory.getLogger(AuthFilter.class);

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (!Config.AUTH_ENABLE) {
            chain.doFilter(request, response);
            return;
        }
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        logger.info(httpRequest.getRemoteAddr() + ": " + httpRequest.getRequestURI());
        String clientToken = httpRequest.getHeader("X-DATAFLOW-TOKEN");
        clientToken = clientToken == null ? "" : clientToken;
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update((clientToken + salt).getBytes("UTF-8"));
            StringBuilder builder = new StringBuilder();
            for (byte bytes: messageDigest.digest()) {
                builder.append(String.format("%02x", bytes & 0xff));
            }
            if (!Config.SERVER_TOKEN.equals(builder.toString())) {
                AuthException ax = new AuthException("Authentication failed");
                httpResponse.sendError(ax.getStatus().value(), ax.getDescription());
                return;
            } else {
                chain.doFilter(request, response);
            }
        } catch (NoSuchAlgorithmException e) {
            throw new InternalError("Fail to find auth algorithm");
        }
    }

    @Override
    public void destroy() {
    }
}
