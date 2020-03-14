package com.sixestates.crawler.agent;

import com.sixestates.crawler.config.BaseConfig;

public class Config extends BaseConfig {
	private static final String Cluster_Module_Port_Key = "cluster.module.port";

	private static Integer Cluster_Module_Port = null;

	public static int getCluster_Module_Port() {
		return Cluster_Module_Port;
	}

	@Override
	protected void initFromEnv() {
		String port = System.getenv(Cluster_Module_Port_Key);
		if (port != null && !port.isEmpty()) {
			try {
				Cluster_Module_Port = Integer.parseInt(port);
			} catch (NumberFormatException e) {
				// ignore
			}
		}
	}

	@Override
	protected boolean verify() {
		if (Cluster_Module_Port == null) {
			errorMessage.append(Cluster_Module_Port_Key).append("is null\n");
			return false;
		}
		return true;
	}

	@Override
	protected void genConfigMessage() {
		configMessage.append("\t").append(String.valueOf(Cluster_Module_Port_Key)).append("\t")
				.append(String.valueOf(Cluster_Module_Port)).append("\n");
	}
}
