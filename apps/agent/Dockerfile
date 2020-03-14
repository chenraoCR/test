FROM openjdk:8-jre-slim
ARG version
WORKDIR /app
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo "Asia/Shanghai" > /etc/timezone
COPY apps/agent/target/6crawler-agent-${version}-all.jar ./6crawler-agent.jar
CMD ["java", "-jar", "6crawler-agent.jar"]