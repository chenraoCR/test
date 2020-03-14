package com.sixestates.crawler.infoprovider.utils;

import com.lakeside.core.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by @maximin.
 */
public class Config {

    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    public static final String AUTH_ENABLE_KEY = "auth.enable";
    public static final String SERVER_PORT_KEY = "server.port";
    public static final String SERVER_TOKEN_KEY = "server.token";

    public static boolean AUTH_ENABLE;
    public static String SERVER_PORT;
    public static String SERVER_TOKEN;

    public static boolean initConfig() {
        init();
        return verify();
    }

    private static void init() {
        if (System.getenv(AUTH_ENABLE_KEY) == null) {
            AUTH_ENABLE = false;
        } else {
            AUTH_ENABLE = Boolean.valueOf(System.getenv(AUTH_ENABLE_KEY));
        }
        SERVER_PORT = System.getenv(SERVER_PORT_KEY);
        SERVER_TOKEN = System.getenv(SERVER_TOKEN_KEY);
        SERVER_TOKEN = System.getenv(SERVER_TOKEN_KEY);
    }

    private static boolean verify() {
        if (SERVER_PORT == null) {
            logger.error("no env variable {} provided", SERVER_PORT_KEY);
            return false;
        }
        if (AUTH_ENABLE && (StringUtils.isEmpty(SERVER_TOKEN)) ) {
            logger.error("no env variable {} provided", SERVER_TOKEN_KEY);
            return false;
        }
        print();
        return true;
    }

    private static void print() {
        StringBuilder builder = new StringBuilder();
        builder.append("\n\n");
        builder.append("\t" + AUTH_ENABLE_KEY + ":\t" + AUTH_ENABLE + "\n");
        builder.append("\t" + SERVER_PORT_KEY + ":\t" + SERVER_PORT + "\n");
        logger.info(builder.toString());
    }
}
