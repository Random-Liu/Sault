package com.pku.ebolt.util;

import java.io.Serializable;

public class EBoltConfig implements Serializable {
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
	private String applicationName = "EBolt";
	
	public EBoltConfig() {}

	public int getNodeNumber() {
		return nodeNumber;
	}

	public EBoltConfig setNodeNumber(int nodeNumber) {
		this.nodeNumber = nodeNumber;
		return this;
	}
	
	public int getResourceOfNode() {
		return resourceOfNode;
	}

	public EBoltConfig setResourceOfNode(int resourceOfNode) {
		this.resourceOfNode = resourceOfNode;
		return this;
	}
	
	public String getSparkMaster() {
		return sparkMaster;
	}

	public EBoltConfig setSparkMaster(String sparkMaster) {
		this.sparkMaster = sparkMaster;
		return this;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public EBoltConfig setApplicationName(String applicationName) {
		this.applicationName = applicationName;
		return this;
	}
}
