FROM openjdk:8-jre-slim
ARG version
WORKDIR /app
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo "Asia/Shanghai" > /etc/timezone
COPY apps/info-provider/target/6crawler-info-provider-${version}.jar ./6crawler-info-provider.jar
CMD ["java", "-jar", "6crawler-info-provider.jar"]