package com.pku.ebolt.engine.operator;

import java.io.Serializable;

import com.pku.ebolt.api.Tuple;

class TupleWrapper implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Tuple tuple;
	
	TupleWrapper(Tuple tuple) {
		this.tuple = tuple;
	}
	
	@Override
	public int hashCode() {
		// Return hashCode of key
		return tuple.getKey().hashCode();
	}
	
	@Override
	// Override this for hash map
	public boolean equals(Object tupleWrapper) {
		assert(tupleWrapper instanceof TupleWrapper);
		Object key1 = tuple.getKey();
		Object key2 = ((TupleWrapper)tupleWrapper).tuple.getKey();
		return key1.equals(key2);
	}
}