package com.pku.sault.engine.operator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.Map.Entry;

import akka.actor.ActorRef;

class RouteTree implements Serializable {
	private static final long serialVersionUID = 1L;

	private class RouteTreeNode implements Serializable {
		private static final long serialVersionUID = 1L;
		
		int upperBound;
		ActorRef target;
		RouteTreeNode (int upperBound, ActorRef target) {
			this.upperBound = upperBound;
			this.target = target;
		}
	}
	
	TreeMap<Integer, RouteTreeNode> routeMap;

	RouteTree(Iterable<ActorRef> targets) {
		Queue<Integer> lowerBounds = new LinkedList<Integer>();
		Iterator<ActorRef> targetIter = targets.iterator();
		assert(targetIter.hasNext());
		routeMap = new TreeMap<Integer, RouteTreeNode>();
		routeMap.put(0, new RouteTreeNode(Integer.MAX_VALUE, targetIter.next()));
		lowerBounds.offer(0);
		while (targetIter.hasNext()) {
			int lowerBound = lowerBounds.poll();
			assert(canBeSplit(lowerBound));
			lowerBounds.offer(lowerBound);
			lowerBounds.offer(split(lowerBound, targetIter.next()));
		}
	}
	
	Map<ActorRef, Integer> createTargetRanges() {
		Map<ActorRef, Integer> targetRanges = new HashMap<ActorRef, Integer>();
		for (Entry<Integer, RouteTreeNode> targetEntry : routeMap.entrySet())
			targetRanges.put(targetEntry.getValue().target, targetEntry.getKey());
		return targetRanges;
	}
	
	ActorRef route(TupleWrapper tupleWrapper) {
		int key = tupleWrapper.getKey().hashCode();
		// We can only handle positive number current now
		assert key >= 0;

		Entry<Integer, RouteTreeNode> targetTableItem = routeMap.floorEntry(key);
		return targetTableItem.getValue().target;
	}
	
	ActorRef getTarget(int lowerBound) {
		assert(isValidLowerBound(lowerBound));
		
		return routeMap.get(lowerBound).target;
	}
	
	// Set new target and return old target
	ActorRef setTarget(int lowerBound, ActorRef newTarget) {
		assert(isValidLowerBound(lowerBound));
		RouteTreeNode routeTreeNode = routeMap.get(lowerBound);
		ActorRef oldTarget = routeTreeNode.target;
		routeTreeNode.target = newTarget;
		return oldTarget;
	}
	
	boolean isValidLowerBound(int lowerBound) {
		return routeMap.containsKey(lowerBound);
	}
	
	// If lowerBound == upperBound, the range can not be split
	boolean canBeSplit(int lowerBound) {
		assert(isValidLowerBound(lowerBound));
		
		return lowerBound < routeMap.get(lowerBound).upperBound;
	}
	
	// Set target in new sub range, return new lowerBound
	int split(int lowerBound, ActorRef target) {
		assert(isValidLowerBound(lowerBound));
		assert(target != null);
		
		RouteTreeNode oldRouteTreeNode = routeMap.get(lowerBound);
		int upperBound = oldRouteTreeNode.upperBound;
		
		assert(lowerBound < upperBound); // unit should never be split

		// This will exceed integer limit
		// int middle = (lowerBound + upperBound) / 2;
		// Because lowerBound must be even:
		// 	(lowerBound / 2 + upperBound / 2) == (lowerBound + upperBound) / 2
		assert(lowerBound % 2 == 0);
		int middle = (lowerBound / 2 + upperBound / 2);
		RouteTreeNode newRouteTreeNode = new RouteTreeNode(upperBound, target);
		oldRouteTreeNode.upperBound = middle;
		routeMap.put(middle + 1, newRouteTreeNode);
		return middle + 1;
	}
	
	// Is sibling exists (It may be split or [MIN_VALUE, 0))
	boolean isSiblingAvailable(int lowerBound) {
		assert(isValidLowerBound(lowerBound));

		int siblingLowerBound = siblingNode(lowerBound);
		return (siblingLowerBound >= 0 && !isSiblingSplit(siblingLowerBound, lowerBound));
	}
	
	// Remove target in given range. Return new lowerBound after merging, if you want to update
	// lowerBound of the old target, we can just send a message to it.
	// If failed, return -1
	int merge(int lowerBound) {
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
			routeMap.get(siblingLowerBound).upperBound = routeMap.get(lowerBound).upperBound;
			routeMap.remove(lowerBound);
			return siblingLowerBound;
		}
	}
	
	// Return sibling target, if sibling is split return null
	ActorRef sibling(int lowerBound) {
		int siblingLowerBound = siblingNode(lowerBound);
		// If siblingLowerBound == MIN_VALUE, it means that lowerBound is 0-MAX_VALUE,
		// it has no siblings.
		if (siblingLowerBound == Integer.MIN_VALUE || isSiblingSplit(siblingLowerBound, lowerBound)) {
			System.err.println("Sibling is split or not exist");
			return null;
		}
		return routeMap.get(siblingLowerBound).target;
	}
	
	// Return whether sibling node is split
	private boolean isSiblingSplit(int siblingLowerBound, int lowerBound) {
		int siblingUpperBound = routeMap.get(siblingLowerBound).upperBound;
		if (siblingLowerBound > lowerBound)
			return (siblingUpperBound - siblingLowerBound) != (siblingLowerBound - 1 - lowerBound);
		else
			return siblingUpperBound != lowerBound - 1;
	}
	
	// Return sibling lower bound, if lowerBound == 0 and upperBound == MAX_VALUE
	// the function will return MIN_VALUE
	// It is sure that:
	// 		siblingNode(lowerBound) != lowerBound
	private int siblingNode(int lowerBound) {
		RouteTreeNode routeTreeNode = routeMap.get(lowerBound);
		int upperBound = routeTreeNode.upperBound;
		
		int x = lowerBound ^ (upperBound + 1); // x
		if ((x & (x-1)) == 0) // Left Node
			// if (upperBound == Integer.MAX_VALUE)
			// upperBound + 1 == Integer.MIN_VALUE
			return upperBound + 1;
		else // Right Node
			return (lowerBound - 1) & lowerBound;
	}
}
