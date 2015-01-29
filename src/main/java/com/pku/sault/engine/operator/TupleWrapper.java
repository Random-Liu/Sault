package com.pku.sault.engine.operator;

import java.io.Serializable;

import com.pku.sault.api.Tuple;

// TODO Can also implement random tuple wrapper by set a random key to tupleWrapper
class TupleWrapper implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Tuple tuple;
	
	TupleWrapper(Tuple tuple) {
		this.tuple = tuple;
	}
	
	Object getKey() {
		return tuple.getKey();
	}

	Tuple getTuple() {
		return tuple;
	}
}