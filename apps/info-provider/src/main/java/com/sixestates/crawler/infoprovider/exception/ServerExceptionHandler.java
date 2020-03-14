package com.sixestates.crawler.infoprovider.exception;

import com.sixestates.crawler.infoprovider.exception.common.InternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author maximin
 */
@ControllerAdvice
public class ServerExceptionHandler {

    private static Logger logger = LoggerFactory.getLogger(ServerExceptionHandler.class);

    public static BaseServerException mapException(Exception ex) {
        BaseServerException ax;

        if (ex instanceof BaseServerException) {
            ax = (BaseServerException) ex;
        } else if (ex.getCause() instanceof BaseServerException) {
            ax = (BaseServerException) ex.getCause();
        } else {
            logger.warn("Unknown exception type: {}", ex.getClass().getName());
            ax = new InternalException(ex);
        }
        return ax;
    }

    @ExceptionHandler(value = {Exception.class})
    public void handleBaseServerException(Exception ex, HttpServletRequest request,
                                          HttpServletResponse response) {

        logger.info("Handling Exception {}", ex.getClass().getName());
        BaseServerException ax = mapException(ex);
        logger.error("Internal Error: {}", ax);

        response.setStatus(ax.getStatus().value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        try {
            response.sendError(ax.getStatus().value(), ax.getDescription());
        } catch (IOException e) {
            logger.error("fail to serve error body: {}", e);
        }
    }

}

