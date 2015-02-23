package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

import akka.actor.ActorRef;

// TODO Use unsigned compare of Integer in comparator of routeMap
class RouteTree implements Serializable {
	private static final long serialVersionUID = 1L;

    private final int MIN_VALUE;
    private final int MAX_VALUE;
	TreeMap<Integer, ActorRef> routeMap;

    // Sub tree used by sub operators
    RouteTree(int lowerBound, int upperBound, ActorRef target) {
        MIN_VALUE = lowerBound;
        MAX_VALUE = upperBound;
        routeMap = new TreeMap<Integer, ActorRef>();
        routeMap.put(MIN_VALUE, target);
    }

    // The whole tree used by operator and up stream sub operators
	RouteTree() {
        MIN_VALUE = 0; // MIN_VALUE = 0 in default
        MAX_VALUE = Integer.MAX_VALUE; // MAX_VALUE = Integer.MAX_VALUE in default
        routeMap = new TreeMap<Integer, ActorRef>();
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
		assert(target != null);

		int upperBound = getUpperBound(lowerBound);
		
		assert(lowerBound < upperBound); // unit should never be split

		// This will exceed integer limit
		// int middle = (lowerBound + upperBound) / 2;
		// Because lowerBound must be even:
		// 	(lowerBound / 2 + upperBound / 2) == (lowerBound + upperBound) / 2
		assert(lowerBound % 2 == 0);
		int middle = (lowerBound / 2 + upperBound / 2);
		routeMap.put(middle + 1, target);
		return middle + 1;
	}

	// Is sibling exists (It may be split or < MIN_VALUE or > MAX_VALUE)
	boolean isSiblingAvailable(int lowerBound) {
		assert(isValidLowerBound(lowerBound));

		int siblingLowerBound = siblingNode(lowerBound);
		return (siblingLowerBound >= MIN_VALUE && siblingLowerBound <= MAX_VALUE
                && !isSiblingSplit(siblingLowerBound, lowerBound));
	}
	
	// Remove target in given range. Return new lowerBound after merging, if you want to update
	// lowerBound of the old target, we can just send a message to it.
	// If failed, return -1
	int merge(int lowerBound) {
        assert isValidLowerBound(lowerBound);
		int siblingLowerBound = siblingNode(lowerBound);
		if (isSiblingSplit(siblingLowerBound, lowerBound)) {
			System.err.println("Sibling is split, can not be merged");
			return -1;
		}
		if (siblingLowerBound > lowerBound) {
			routeMap.put(lowerBound, routeMap.get(siblingLowerBound));
			routeMap.remove(siblingLowerBound);
			return lowerBound;
		} else {
			routeMap.remove(lowerBound);
			return siblingLowerBound;
		}
	}
	
	// Return sibling target, if sibling is split return null
	ActorRef sibling(int lowerBound) {
        assert(isValidLowerBound(lowerBound));
		int siblingLowerBound = siblingNode(lowerBound);
		// If siblingLowerBound < MIN_VALUE or > MAX_VALUE, it means that lowerBound is MIN_VALUE,
		// it has no siblings.
		if (siblingLowerBound < MIN_VALUE || siblingLowerBound > MAX_VALUE
                || isSiblingSplit(siblingLowerBound, lowerBound)) {
			System.err.println("Sibling is split or not exist");
			return null;
		}
		return routeMap.get(siblingLowerBound);
	}

	// Return whether sibling node is split
	private boolean isSiblingSplit(int siblingLowerBound, int lowerBound) {
		int siblingUpperBound = getUpperBound(siblingLowerBound);
		if (siblingLowerBound > lowerBound)
			return (siblingUpperBound - siblingLowerBound) != (siblingLowerBound - 1 - lowerBound);
		else
			return siblingUpperBound != lowerBound - 1;
	}
	
	// Return sibling lower bound, if lowerBound == MIN_VALUE and upperBound == MAX_VALUE
	// the function will return value out of bound
	// It is sure that:
	// 		siblingNode(lowerBound) != lowerBound
	private int siblingNode(int lowerBound) {
		int upperBound = getUpperBound(lowerBound);
		
		int x = lowerBound ^ (upperBound + 1); // x
		if ((x & (x-1)) == 0) // Left Node
			return upperBound + 1;
		else // Right Node
			return (lowerBound - 1) & lowerBound;
	}
}
