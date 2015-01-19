package com.pku.ebolt.engine;

import com.pku.ebolt.api.Tuple;

class TupleWrapper {
	private final int hashCode;
	private final Tuple tuple;
	
	TupleWrapper(Tuple tuple) {
		this.tuple = tuple;
		this.hashCode = tuple.getKey().hashCode(); // Cache the hash code
	}
	
	Tuple getTuple() {
		return tuple;
	}
	
	@Override
	public int hashCode() {
		return this.hashCode;
	}
}