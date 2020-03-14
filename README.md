Introduction
---
6crawler is a server side program which plays the role to dispatch tasks to downloader and parse downloaded pages

To deploy a whole crawler system several process needed to be running: 6crawler-agent, 6crawler-parser, 6crawler-link-modifier, 6crawler-service-provider.

Quick start
---
If you are using **docker** deploy the system is simple.

To deploy 6crawler-agent: `docker run -d --env-file=yourenvfile.env sixestates/sixdocker:6crawler-agent-vx.y.z`

To deploy 6crawler-parser: `docker run -d --env-file=yourenvfile.env sixestates/sixdocker:6crawler-parser-vx.y.z`

To deploy 6crawler-link-modifier: `docker run -d --env-file=yourenvfile.env sixestates/sixdocker:6crawler-link-modifier-vx.y.z`

To deploy 6crawler-service-provider: `docker run -d --env-file=yourenvfile.env sixestates/sixdocker:6crawler-service-provider-vx.y.z`

If you are not using **docker**. You need to run these program via jvm and manually setup environment variables.

To deploy 6crawler-agent: `java -jar 6crawler-agent.jar`

To deploy 6crawler-parser: `java -jar 6crawler-parser.jar`

To deploy 6crawler-link-modifier: `java -jar 6crawler-link-modifier.jar`

To deploy 6crawler-service-provider: `java -jar 6crawler-service-provider.jar`

Docker
---
This project is a maven project, and private repository is needed. So to build the project, you need to setup private maven repository first. After setup private repository, you can build the project with `./build.sh`. By default, build script will push builded image to docker hub. If you don't want auto push to docker hub, just comment related code in build.sh.

Usage
---
The supported environment variables can be found in `example.env`. There is no supported command line arguments right now.

After setup needed environment variables, a simple `java -jar 6crawler-xxx.jar` would run the program.

Architecture
---
The project is a maven project. Multiple maven modules have been organized into a single tree.

* `6crawler`
  * `6crawler-app`
    * `6crawler-agent`
    * `6crawler-parser`
    * `6crawler-link-modifier`
    * `6crawler-service-provider`
    * `6crawler-maintain-tool`
  * `6crawler-libs`
    * `6crawler-model-proxy`
    * `6crawler-model-cache`
    * `6crawler-model-proxy`
    * `6crawler-model-url`
    * `6crawler-model-parser`
    * `6crawler-model-resource`
    * `6crawler-utils`
    * `6crawler-data-storage`
    * `6crawler-config`

The non-leaf nodes in this tree are containers, such modules don't contain any Java code. `6crawler` module setup some properties which is effective over the whole project. And it also manages dependencies and some plugins. `6crawler-app` setup `maven-shade-plugin` to produce executable jar. `6crawler-libs` is a plain container. Modules who are children of `6crawler-libs` are libraries providing various functionality to support other modules.

### 6crawler-agent
This module act as an agent for downloader. It's a WEB service. The route is implemented in `com.sixestates.crawler.agent.mvc.ClusterRouter`. The main jobs of `6crawler-agent` are:
* `/server` downloader register to server via this route.
* `/task/get` downloader fetch task via this route.
* `/task/submit` downloader submit finished task via this route.
* `/task/ua` downloader fetch user-agent from server via this route.

### 6crawler-parser
This module mainly responsible for parsing downloaded web pages. It uses `template-engine` to run scripts to parse pages.

### 6crawler-link-modifier
This module mainly responsible for modifying link status and inserting newly generated links.

### 6crawler-service-provider
This module provides some service to the system including: downloader task timeout repending, parser task timeout repending, garbage collector for some DB resource, checking ***batch*** status and starting new batch when needed.


---
Copyright © [6ESTATES PTE LTD](http://www.6estates.com)
