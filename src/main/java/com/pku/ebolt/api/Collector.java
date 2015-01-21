package com.pku.ebolt.api;

/**
 * Collector Interface.
 * @author taotaotheripper
 *
 */
public interface Collector {
	public void emit(Tuple tuple);
}