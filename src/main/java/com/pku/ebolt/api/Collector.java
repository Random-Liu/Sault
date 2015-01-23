package com.pku.ebolt.api;

/**
 * Collector Interface.
 * @author taotaotheripper
 *
 */
public interface Collector {
	/**
	 * Emit tuple to collector.
	 * @param tuple
	 */
	public void emit(Tuple tuple);
	
	/**
	 * Flush all tuples in collector. Seldom used. 
	 * This is only useful when too much tuples are generated in one execution.
	 */
	public void flush();
}