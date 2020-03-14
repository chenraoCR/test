FROM openjdk:8-jre-slim
ARG version
WORKDIR /app
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo "Asia/Shanghai" > /etc/timezone
COPY apps/parser/target/6crawler-parser-${version}-all.jar ./6crawler-parser.jar
CMD ["java", "-jar", "6crawler-parser.jar"]