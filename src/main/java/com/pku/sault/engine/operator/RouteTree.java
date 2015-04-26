package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

import akka.actor.ActorRef;

// TODO Modify this! Interesting!
// TODO Use unsigned compare of Integer in comparator of routeMap
class RouteTree implements Serializable {
	private static final long serialVersionUID = 1L;

    private final int MIN_VALUE;
    private final int MAX_VALUE;
	TreeMap<Integer, ActorRef> routeMap;

    // The whole tree used by operator and up stream sub operators
	RouteTree() {
        MIN_VALUE = 0; // MIN_VALUE = 0 in default
        MAX_VALUE = Integer.MAX_VALUE; // MAX_VALUE = Integer.MAX_VALUE in default
        routeMap = new TreeMap<Integer, ActorRef>();
	}

	Collection<ActorRef> getTargets() {
		return routeMap.values();
	}

    // TODO Create empty cell
    List<Integer> createEmptyCells(int number) {
        assert routeMap.isEmpty(); // Only used when initialize
        LinkedList<Integer> lowerBounds = new LinkedList<Integer>();
        ActorRef emptyCell = null;
        if (number <= 0) return lowerBounds;
        routeMap.put(MIN_VALUE, emptyCell);
        --number;
        lowerBounds.offer(MIN_VALUE);
        while (number > 0) {
            int lowerBound = lowerBounds.poll();
            assert(canBeSplit(lowerBound));
            lowerBounds.offer(lowerBound);
            lowerBounds.offer(split(lowerBound, emptyCell));
            --number;
        }
        return lowerBounds;
    }
	
	ActorRef route(TupleWrapper tupleWrapper) {
		int key = tupleWrapper.getKey().hashCode();
		// We can only handle positive number current now
		assert key >= MIN_VALUE && key <= MAX_VALUE;

		Entry<Integer, ActorRef> targetTableItem = routeMap.floorEntry(key);
        return targetTableItem.getValue();
	}
	
	ActorRef getTarget(int lowerBound) {
		assert(isValidLowerBound(lowerBound));
		
		return routeMap.get(lowerBound);
	}
	
	// Set new target and return old target
	ActorRef setTarget(int lowerBound, ActorRef newTarget) {
		assert(isValidLowerBound(lowerBound));
		ActorRef oldTarget = routeMap.get(lowerBound);
        routeMap.put(lowerBound, newTarget);
		return oldTarget;
	}
	
	boolean isValidLowerBound(int lowerBound) {
		return routeMap.containsKey(lowerBound);
	}

    // Return the upper bound
    int getUpperBound(int lowerBound) {
        assert isValidLowerBound(lowerBound);
        Integer nextLowerBound = routeMap.higherKey(lowerBound);
        // If there is no next key, it means that the current lower bound
        // is the biggest lower bound, its upper bound must be MAX_VALUE.
        if (nextLowerBound == null) return MAX_VALUE;
        return nextLowerBound - 1;
    }

	// If lowerBound == upperBound, the range can not be split
	boolean canBeSplit(int lowerBound) {
		assert(isValidLowerBound(lowerBound));
		
		return lowerBound < getUpperBound(lowerBound);
	}
	
	// Set target in new sub range, return new lowerBound
	int split(int lowerBound, ActorRef target) {
		assert(isValidLowerBound(lowerBound));
		// assert(target != null); Remove this assertion because the cell may be empty at first and filled later.

		int upperBound = getUpperBound(lowerBound);
		
		assert(lowerBound < upperBound); // unit should never be split

		// This will exceed integer limit
		// int middle = (lowerBound + upperBound) / 2;
		// Because lowerBound must be even:
		// 	(lowerBound / 2 + upperBound / 2) == (lowerBound + upperBound) / 2
		// assert(lowerBound % 2 == 0); Remove this assert because the new route tree can be split and merged whenever
		int middle = (lowerBound / 2 + upperBound / 2);
		routeMap.put(middle + 1, target);
		return middle + 1;
	}
	
	// Merge to left sibling.
	// Remove target in given range. Return new lowerBound after merging, if you want to update
	// lowerBound of the old target, we can just send a message to it.
	// If failed, return -1
	int mergeLeft(int lowerBound) {
        assert isValidLowerBound(lowerBound);
		Integer leftSiblingLowerBound = routeMap.lowerKey(lowerBound);
		if (leftSiblingLowerBound == null) return -1;
		routeMap.remove(lowerBound);
		return leftSiblingLowerBound;
	}

	// Merge to right sibling
	// If failed, return -1
	int mergeRight(int lowerBound) {
		assert isValidLowerBound(lowerBound);
		Entry<Integer, ActorRef> rightSiblingEntry = routeMap.higherEntry(lowerBound);
		if (rightSiblingEntry == null) return -1;
		int rightLowerBound = rightSiblingEntry.getKey();
		ActorRef rightTarget = rightSiblingEntry.getValue();
		routeMap.put(lowerBound, rightTarget);
		routeMap.remove(rightLowerBound);
		return lowerBound;
	}

	ActorRef leftSibling(int lowerBound) {
		assert(isValidLowerBound(lowerBound));
		Entry<Integer, ActorRef> leftSiblingEntry = routeMap.lowerEntry(lowerBound);
		if (leftSiblingEntry == null) return null; // it is the left most node
		return leftSiblingEntry.getValue();
	}

	ActorRef rightSibling(int lowerBound) {
		assert(isValidLowerBound(lowerBound));
		Entry<Integer, ActorRef> rightSiblingEntry = routeMap.higherEntry(lowerBound);
		if (rightSiblingEntry == null) return null; // it is the left most node
		return rightSiblingEntry.getValue();
	}
}
