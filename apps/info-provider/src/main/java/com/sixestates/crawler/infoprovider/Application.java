package com.sixestates.crawler.infoprovider;

import com.sixestates.crawler.config.BaseConfig;
import com.sixestates.crawler.infoprovider.filter.AuthFilter;
import com.sixestates.crawler.infoprovider.utils.Config;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import javax.servlet.Filter;

/**
 * Created by @maximin.
 */
@Configuration
@ComponentScan
public class Application extends SpringBootServletInitializer{

    public static void main(String[] args) {
        if (BaseConfig.initConfig() && Config.initConfig()) {
            SpringApplication.run(Application.class, args);
        } else {
            System.exit(1);
        }
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(ServerAppConfiguration.class);
    }

    @Bean
    public FilterRegistrationBean<Filter> filterRegistrationBean() {
        return new FilterRegistrationBean<>(new AuthFilter());
    }

    @Configuration
    @EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class})
    public class ServerAppConfiguration {
    }
}
