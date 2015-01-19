package com.pku.ebolt.engine;

import java.util.Iterator;
import java.util.LinkedList;

import com.pku.ebolt.api.Tuple;

class Collector implements com.pku.ebolt.api.Collector {
	LinkedList<Tuple> emitBuffer;

	Collector() {
		emitBuffer = new LinkedList<Tuple>();
	}
	
	final public void emit(Tuple tuple) {
		emitBuffer.add(tuple);
	}
	
	Iterator<Tuple> getBufferIterator() {
		return emitBuffer.iterator();
	}
	
	void cleanBuffer() {
		emitBuffer.clear();
	}
}
