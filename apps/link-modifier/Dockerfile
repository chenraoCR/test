FROM openjdk:8-jre-slim
ARG version
WORKDIR /app
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo "Asia/Shanghai" > /etc/timezone
COPY /apps/link-modifier/target/6crawler-link-modifier-${version}-all.jar ./6crawler-link-modifier.jar
CMD ["java", "-jar", "6crawler-link-modifier.jar"]