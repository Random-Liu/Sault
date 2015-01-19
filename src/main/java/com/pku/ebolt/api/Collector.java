package com.pku.ebolt.api;

public interface Collector {
	public void emit(Tuple tuple);
}