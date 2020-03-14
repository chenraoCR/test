package com.sixestates.crawler.linkmodifier;

import java.util.Optional;

import com.sixestates.crawler.config.BaseConfig;

public class Config extends BaseConfig {
	private static final String Cluster_Task_Link_Batch_Size_Key = "cluster.task.link.batch.size";
	private static final String Cluster_Page_Retry_Max_Key = "cluster.page.retry.max";

	private static int Cluster_Task_Link_Batch_Size = 20;
	private static int Cluster_Page_Retry_Max = 40;

	@Override
	protected void initFromEnv() {
		Optional<String> batch_size = Optional.ofNullable(System.getenv(Cluster_Task_Link_Batch_Size_Key));
		if (batch_size.isPresent() && !batch_size.get().isEmpty()) {
			try {
				Cluster_Task_Link_Batch_Size = Integer.parseInt(batch_size.get());
			} catch (NumberFormatException e) {
				Cluster_Task_Link_Batch_Size = 20;
			}
		}
		Optional<String> page_retry_max = Optional.ofNullable(System.getenv(Cluster_Page_Retry_Max_Key));
		if (page_retry_max.isPresent() && !page_retry_max.get().isEmpty()) {
			try {
				Cluster_Page_Retry_Max = Integer.parseInt(page_retry_max.get());
			} catch (NumberFormatException e) {
				Cluster_Page_Retry_Max = 40;
			}
		}
	}

	@Override
	protected boolean verify() {
		return true;
	}

	@Override
	protected void genConfigMessage() {
		configMessage.append("\t" + Cluster_Task_Link_Batch_Size_Key + ": \t " + Cluster_Task_Link_Batch_Size + "\n");
		configMessage.append("\t" + Cluster_Page_Retry_Max_Key + ": \t " + Cluster_Page_Retry_Max + "\n");
	}

	public static int getCluster_Task_Link_Batch_Size() {
		return Cluster_Task_Link_Batch_Size;
	}

	public static int getCluster_Page_Retry_Max() {
		return Cluster_Page_Retry_Max;
	}
}
