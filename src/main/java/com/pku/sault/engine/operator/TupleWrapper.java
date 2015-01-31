package com.pku.sault.engine.operator;

import java.io.Serializable;

import com.pku.sault.api.Tuple;

// TODO Can also implement random tuple wrapper by set a random key to tupleWrapper
class TupleWrapper implements Serializable {
	private static final long serialVersionUID = 1L;
	private final KeyWrapper key;
	private final Tuple tuple;

	TupleWrapper(Tuple tuple) {
		this.tuple = tuple;
		this.key = new KeyWrapper(tuple.getKey());
	}
	
	KeyWrapper getKey() {
		return this.key;
	}

	Tuple getTuple() {
		return tuple;
	}
}

class KeyWrapper implements Serializable {
	private final int hash;
	private final Object key;
	KeyWrapper(Object key) {
		this.key = key;
		// 1. Do hash in constructor, so that hash calculation will be done in worker rather than router.
		// 2. Do rehash to uniform distribute the hash key in 0-INT_MAX
		this.hash = rehash(key.hashCode()) & 0x7FFFFFFF;
	}

	@Override
	public int hashCode() {
		return this.hash;
	}

	@Override
	public boolean equals(Object keyWrapper) {
		return this.key.equals(((KeyWrapper)keyWrapper).key);
	}

	private int rehash(int a)
	{
		a += ~(a<<15);
		a ^=  (a>>10);
		a +=  (a<<3);
		a ^=  (a>>6);
		a += ~(a<<11);
		a ^=  (a>>16);
		return a;
	}
}