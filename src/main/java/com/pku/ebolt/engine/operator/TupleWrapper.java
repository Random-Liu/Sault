package com.pku.ebolt.engine.operator;

import java.io.Serializable;

import com.pku.ebolt.api.Tuple;

class TupleWrapper implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Tuple tuple;
	
	TupleWrapper(Tuple tuple) {
		this.tuple = tuple;
	}
	
	Object getKey() {
		return tuple.getKey();
	}
}