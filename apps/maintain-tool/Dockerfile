FROM openjdk:8-jre-slim
ARG version
WORKDIR /app
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo "Asia/Shanghai" > /etc/timezone
COPY apps/maintain-tool/target/6crawler-maintain-tool-${version}-all.jar ./6crawler-maintain-tool.jar
ENTRYPOINT ["java", "-jar", "6crawler-maintain-tool.jar", "com.sixestates.crawler.maintaintool.Runner"]