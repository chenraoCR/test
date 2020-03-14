package com.sixestates.crawler.parser;

import java.util.Optional;

import com.sixestates.crawler.config.BaseConfig;

public class Config extends BaseConfig {
	private static final String Cluster_Task_Parse_Threads_Key = "cluster.task.parse.threads";
	private static final String Cluster_Page_Parse_Threads_Key = "cluster.page.parse.threads";
	private static final String Cluster_Task_Get_Num_Key = "cluster.task.get.num";
	private static final String Cluster_Page_Layer_Max_Key = "cluster.page.layer.max";
	private static final String Cluster_Template_Load_From_Key = "cluster.template.load.from";
	private static final String Cluster_Template_Reload_Interval_Key = "cluster.template.reload.interval";
	private static final String Cluster_Template_Reload_Listen_Channel_Key = "cluster.template.reload.listen.channel";
	private static final String Kafka_Cluster_Name_Key = "kafka.cluster.name";

	private static int Cluster_Task_Parse_Threads = 1;
	private static int Cluster_Page_Parse_Threads = 25;
	private static int Cluster_Task_Get_Num = 5;
	private static int Cluster_Page_Layer_Max = 100;
	private static String Cluster_Template_Load_From = "db";
	private static int Cluster_Template_Reload_Interval = 3600;
	private static String Cluster_Template_Reload_Listen_Channel = "tpl_reload_channel";
	private static String Kafka_Cluster_Name = "6e";

	@Override
	protected void initFromEnv() {
		Optional<String> task_threads = Optional.ofNullable(System.getenv(Cluster_Task_Parse_Threads_Key));
		if (task_threads.isPresent() && !task_threads.get().isEmpty()) {
			Cluster_Task_Parse_Threads = Integer.parseInt(task_threads.get());
		}
		Optional<String> page_threads = Optional.ofNullable(System.getenv(Cluster_Page_Parse_Threads_Key));
		if (page_threads.isPresent() && !page_threads.get().isEmpty()) {
			Cluster_Page_Parse_Threads = Integer.parseInt(page_threads.get());
		}
		Optional<String> task_get_num = Optional.ofNullable(System.getenv(Cluster_Task_Get_Num_Key));
		if (task_get_num.isPresent() && !task_get_num.get().isEmpty()) {
			Cluster_Task_Get_Num = Integer.parseInt(task_get_num.get());
		}
		Optional<String> page_layer = Optional.ofNullable(System.getenv(Cluster_Page_Layer_Max_Key));
		if (page_layer.isPresent() && !page_layer.get().isEmpty()) {
			Cluster_Page_Layer_Max = Integer.parseInt(page_layer.get());
		}
		Optional<String> template_load = Optional.ofNullable(System.getenv(Cluster_Template_Load_From_Key));
		if (template_load.isPresent() && !template_load.get().isEmpty()) {
			Cluster_Template_Load_From = template_load.get();
		}
		Optional<String> template_reload_interval = Optional
				.ofNullable(System.getenv(Cluster_Template_Reload_Interval_Key));
		if (template_reload_interval.isPresent() && !template_reload_interval.get().isEmpty()) {
			Cluster_Template_Reload_Interval = Integer.parseInt(template_reload_interval.get());
		}
		Optional<String> template_reload_listen_channel = Optional
				.ofNullable(System.getenv(Cluster_Template_Reload_Listen_Channel_Key));
		if (template_reload_listen_channel.isPresent() && !template_reload_listen_channel.get().isEmpty()) {
			Cluster_Template_Reload_Listen_Channel = template_reload_listen_channel.get();
		}
		Optional<String> kafka_cluster_name = Optional.ofNullable(System.getenv(Kafka_Cluster_Name_Key));
		if (kafka_cluster_name.isPresent() && !kafka_cluster_name.get().isEmpty()) {
			Kafka_Cluster_Name = kafka_cluster_name.get();
		}
	}

	@Override
	protected boolean verify() {
		if (Cluster_Task_Parse_Threads <= 0) {
			errorMessage.append(Cluster_Task_Parse_Threads_Key + " number is <= 0.\n");
			return false;
		}
		if (Cluster_Page_Parse_Threads <= 0) {
			errorMessage.append(Cluster_Page_Parse_Threads_Key + " number is <= 0.\n");
			return false;
		}
		if (Cluster_Task_Get_Num <= 0) {
			errorMessage.append(Cluster_Task_Get_Num_Key + " number is <= 0.\n");
			return false;
		}
		if (Cluster_Page_Layer_Max <= 0) {
			errorMessage.append(Cluster_Page_Layer_Max_Key + " number is <= 0.\n");
			return false;
		}
		if (Cluster_Template_Reload_Interval <= 0) {
			errorMessage.append(Cluster_Template_Reload_Interval_Key + " number is <= 0.\n");
			return false;
		}
		return true;
	}

	@Override
	protected void genConfigMessage() {
		configMessage.append("\t" + Cluster_Task_Parse_Threads_Key + ": \t " + Cluster_Task_Parse_Threads + "\n");
		configMessage.append("\t" + Cluster_Page_Parse_Threads_Key + ": \t " + Cluster_Page_Parse_Threads + "\n");
		configMessage.append("\t" + Cluster_Task_Get_Num_Key + ": \t " + Cluster_Task_Get_Num + "\n");
		configMessage.append("\t" + Cluster_Page_Layer_Max_Key + ": \t " + Cluster_Page_Layer_Max + "\n");
		configMessage.append("\t" + Cluster_Template_Load_From_Key + ": \t " + Cluster_Template_Load_From + "\n");
		configMessage.append(
				"\t" + Cluster_Template_Reload_Interval_Key + ": \t " + Cluster_Template_Reload_Interval + "\n");
		configMessage.append("\t" + Cluster_Template_Reload_Listen_Channel_Key + ": \t "
				+ Cluster_Template_Reload_Listen_Channel + "\n");
		configMessage.append("\t" + Kafka_Cluster_Name_Key + ": \t " + Kafka_Cluster_Name + "\n");
	}

	public static int getCluster_Task_Parse_Threads() {
		return Cluster_Task_Parse_Threads;
	}

	public static int getCluster_Page_Parse_Threads() {
		return Cluster_Page_Parse_Threads;
	}

	public static int getCluster_Page_Layer_Max() {
		return Cluster_Page_Layer_Max;
	}

	public static String getCluster_Template_Load_From() {
		return Cluster_Template_Load_From;
	}

	public static int getCluster_Template_Reload_Interval() {
		return Cluster_Template_Reload_Interval;
	}

	public static String getCluster_Template_Reload_Listen_Channel() {
		return Cluster_Template_Reload_Listen_Channel;
	}

	public static int getCluster_Task_Get_Num() {
		return Cluster_Task_Get_Num;
	}

	public static String getKafka_Cluster_Name() {
		return Kafka_Cluster_Name;
	}
}
