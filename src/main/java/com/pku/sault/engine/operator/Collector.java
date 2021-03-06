package com.pku.sault.engine.operator;

import java.util.Iterator;
import java.util.LinkedList;

import akka.actor.ActorRef;

import com.pku.sault.api.Tuple;

class Collector implements com.pku.sault.api.Collector {
	private LinkedList<Tuple> emitBuffer;
	private ActorRef outputRouter;
	private ActorRef worker;
	
	Collector(ActorRef outputRouter, ActorRef worker) {
		this.outputRouter = outputRouter;
		this.worker = worker;
		emitBuffer = new LinkedList<Tuple>();
	}
	
	final public void emit(Tuple tuple) {
		emitBuffer.add(tuple);
	}
	
	// TODO Can be optimized later, such as aggregation etc.
	final public void flush() {
		Iterator<Tuple> tupleIter = emitBuffer.iterator();
		while (tupleIter.hasNext())
			outputRouter.tell(new TupleWrapper(tupleIter.next()), worker);
		emitBuffer.clear();
	}
}
