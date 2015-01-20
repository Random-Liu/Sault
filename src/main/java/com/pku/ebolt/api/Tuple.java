package com.pku.ebolt.api;

import java.io.Serializable;

/*
 * All use Object current now
 */
public class Tuple implements Serializable {
	private static final long serialVersionUID = 1L;
	
	final private Object key;
	final private Object value;
	
	public Tuple(Object key, Object value) {
		this.key = key;
		this.value = value;
	}
	
	public Object getKey() { return key; }
	public Object getValue() { return value; }
}
