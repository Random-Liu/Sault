package com.pku.sault.util;

import java.io.Serializable;

public class SaultConfig implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/**
	 * Resource Configuration
	 */
	private int nodeNumber = 16;
	private int resourceOfNode = 16;
	private String sparkMaster = "local[16]";
	
	/**
	 * Application Configuration
	 */
	private String applicationName = "Sault";
	
	public SaultConfig() {}

	public int getNodeNumber() {
		return nodeNumber;
	}

	public SaultConfig setNodeNumber(int nodeNumber) {
		this.nodeNumber = nodeNumber;
		return this;
	}
	
	public int getResourceOfNode() {
		return resourceOfNode;
	}

	public SaultConfig setResourceOfNode(int resourceOfNode) {
		this.resourceOfNode = resourceOfNode;
		return this;
	}
	
	public String getSparkMaster() {
		return sparkMaster;
	}

	public SaultConfig setSparkMaster(String sparkMaster) {
		this.sparkMaster = sparkMaster;
		return this;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public SaultConfig setApplicationName(String applicationName) {
		this.applicationName = applicationName;
		return this;
	}
}
