package com.pku.ebolt.api;

/*
 * All use Object current now
 */
public class Tuple {
	final private Object key;
	final private Object value;
	
	public Tuple(Object key, Object value) {
		this.key = key;
		this.value = value;
	}
	
	public Object getKey() { return key; }
	public Object getValue() { return value; }
}
