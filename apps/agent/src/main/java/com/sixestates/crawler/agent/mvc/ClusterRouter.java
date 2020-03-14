package com.sixestates.crawler.agent.mvc;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.lakeside.core.utils.EncodeUtils;
import com.lakeside.web.BaseController;
import com.sixestates.crawler.model.queue.TaskQueue;

@Controller
@RequestMapping("/cluster")
public class ClusterRouter extends BaseController {
	private static final Logger logger = LoggerFactory.getLogger(ClusterRouter.class);

	@RequestMapping("/external/batch/registry")
	@ResponseBody
	public Map<String, Object> registerExternalBatch(HttpServletRequest request,
			@RequestParam(value="token", required=true) long token) {
		logger.info("external fast-agent: {} is registering ...", token);
		return ClusterService.registerExternalBatch(token);
	}

	@RequestMapping("/external/batch/status")
	@ResponseBody
	public Map<String, Object> getExternalBatch(HttpServletRequest request,
			@RequestParam(value="token", required=true) long token,
			@RequestParam(value="external_id", required=true) long batchId) {
		logger.info("external fast-agent: {} is getting status of batch: {}", token, batchId);
		return ClusterService.getExternalBatch(batchId);
	}

	@RequestMapping("/external/batch/close")
	@ResponseBody
	public Map<String, Object> terminateExternalBatch(HttpServletRequest request,
			@RequestParam(value = "token", required = true) long token,
			@RequestParam(value = "external_id", required = true) long batchId) {
		logger.info("external fast-agent: {} is terminating a batch: {}", token, batchId);
		return ClusterService.terminateExternalBatch(token, batchId);
	}

	@RequestMapping("/server")
	@ResponseBody
	public Map<String, Object> getServer(HttpServletRequest request,
			@RequestParam(value = "identity", required = true) String identity,
			@RequestParam(value = "cpu_core", required = true) int cpuCore,
			@RequestParam(value = "ram", required = true) double ram,
			@RequestParam(value = "version", required = true) String version) {

		logger.info("agenthost {} registering...", identity);
		try {
			Map<String, Object> serverList = new HashMap<>();
			Long token = ClusterService.register(identity, cpuCore, ram, version);
			serverList.put("token", token.toString());
			return serverList;
		} catch (Exception e) {
			logger.error("unexpected exception", e);
			throw new RuntimeException(e);
		}
	}

	@RequestMapping("/task/get")
	@ResponseBody
	public Map<String, Object> getTask(
			@RequestParam(value = "token", required = true) long token,
			@RequestParam(value = "include", required = false, defaultValue = "") String[] includes,
			@RequestParam(value = "exclude", required = false, defaultValue = "") String[] excludes) {

		logger.info("agenthost get task... token:{}", token);
		try {
			return ClusterService.getTask(token, includes, excludes);
		} catch (Exception e) {
			logger.error("unexpected exception", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * 返回结果总是带有两个field, {"success": false, "errmsg": "..."}
	 * success标记提交的文件是否已经存妥, errmsg附带额外的信息, 如果一切正常, errmsg就是空的
	 * success如果是true, 但是errmsg非空, 则意味着文件虽然存好了, 但是有一些错误
	 * 目前用这个表达服务器过载, 希望agent能够暂停获取新任务
	 */
	@RequestMapping(value = "/task/submit", method = RequestMethod.POST)
	@ResponseBody
	public Map<String, Object> submit(
			@RequestParam(value = "token", required = true) long token,
			@RequestParam(value = "query_ids", required = false) List<Long> queryIds,
			@RequestParam(value = "md5_checksum", required = false) String md5,
			@RequestParam(value = "inquire", required = true, defaultValue = "False") boolean inquire, // this is an inquire request
			@RequestParam(value = "file", required = false) MultipartFile file,
			HttpServletRequest request) {
		Map<String, Object> result = new HashMap<>();
		// ask sever status
		if (inquire) {
			result.put("success", true);
			if (TaskQueue.get().overloaded()) {
				result.put("errmsg", "the task queue still being overloaded");
			} else {
				result.put("errmsg", "");
			}
			return result;
		}
		logger.info("agenthost submit task... token:{}, query_ids:{}", token, queryIds);
		if (file.isEmpty()) {
			logger.error("task file is empty agenthost {}", token);
			result.put("success", false);
			result.put("errmsg", "file is empty");
			return result;
		}
		logger.info("Submiting Task token:{}", token);
		byte[] bytes = null;
		try {
			bytes = file.getBytes();
		} catch (IOException e) {
			logger.warn("get bytes from file failed", e);
			result.put("success", false);
			result.put("errmsg", e.getMessage());
			return result;
		}
		String md5Encode = EncodeUtils.md5Encode(bytes);
		if (!md5.equals(md5Encode)) {
			logger.warn("submited Task file check failed, token:{}, upload md5:{}, calculated md5:{}", token, md5, md5Encode);
			result.put("success", false);
			result.put("errmsg", "checksum miss match");
			return result;
		}
		long taskId = ClusterService.submitTask(token, queryIds, md5, bytes);
		logger.info("Submit task succeed, token:{}, task_id:{}", token, taskId);
		result.put("success", true);
		if (TaskQueue.get().overloaded()) {
			result.put("errmsg", "the task queue is overloaded");
		} else {
			result.put("errmsg", "");
		}
		return result;
	}

	@RequestMapping(value = "/task/ua")
	@ResponseBody
	public List<Object> getUserAgents(HttpServletRequest request) {
		return ClusterService.getUserAgents();
	}
}
