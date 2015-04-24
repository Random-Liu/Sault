package com.pku.sault.api;

import java.io.Serializable;

/**
 * Configuration API.
 * @author taotaotheripper
 *
 */
public class Config implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/**
	 * Resource Configuration
	 */
	private int nodeNumber = 12;
	private int resourceOfNode = 16;
	private String sparkMaster = "local[16]";
	
	/**
	 * Application Configuration
	 */
	private String applicationName = "Sault";
	
	public Config() {}

	public int getNodeNumber() {
		return nodeNumber;
	}

	public Config setNodeNumber(int nodeNumber) {
		this.nodeNumber = nodeNumber;
		return this;
	}
	
	public int getResourceOfNode() {
		return resourceOfNode;
	}

	public Config setResourceOfNode(int resourceOfNode) {
		this.resourceOfNode = resourceOfNode;
		return this;
	}
	
	public String getSparkMaster() {
		return sparkMaster;
	}

	public Config setSparkMaster(String sparkMaster) {
		this.sparkMaster = sparkMaster;
		return this;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public Config setApplicationName(String applicationName) {
		this.applicationName = applicationName;
		return this;
	}
}
