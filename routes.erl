% This module manages a dict-based data structure for gateway routing
% tables. The overall structure is:
% Destination -> [{Router, Metric}|_]
% Destination is the node name of the destination.
% Router is the worker process that represents the immediate peer that can route to Destination.
% Metric is the cost metric associated with the available route.
% The "router/metric" list may contain zero or more {Router, Metric} tuples, always in ascending order of Metric.
% The best route to Destination at any time is always the head of the associated list.
% The suboptimal routes are stored in case a link goes down. In such an event the other routes will serve as backups.
% The algorithm that is used to maintain the routing table is a link vector algorithm.
% Note that this can result in temporary routing loops when nodes go down, or a period of time
% between when a node comes up and when it is routable from everywhere.
% Routing loops are currently not handled at all. We simply assume that the routing protocol will correct the loop while the message is in transit, allowing it to find its destination.
% Incomplete routing tables are handled by dropping messages, so messaging is not guaranteed.
% It might be nice to implement that eventually.
%
% Wishlist:
% TTL and/or path based loop detection to try to escape routing loops
% Node failure detection
% Guaranteed messaging

-module(routes).

-export([add/5, remove_through/3, route_to/2, best/2]).

% Calls Callback with destination and best metric as reachable from this node.
best(Routes, Callback) -> best_list(dict:to_list(Routes), Callback).
best_list([], _) -> ok;
best_list([{_, []}|Rest], Callback) -> best_list(Rest, Callback);
best_list([{Dest, [{_, Metric}|_]}|Rest], Callback) ->
	Callback(Dest, Metric),
	best_list(Rest, Callback).

% returns {ok, Router} or no_route
route_to(ToID, Routes) ->
	case dict:find(ToID, Routes) of
		{ok, [{Router, Metric}|_]} when Metric =/= infinity -> {ok, Router};
		_ -> no_route
	end.

add(Dest, Router, Metric, Routes, NotifyPeers) ->
	case dict:find(Dest, Routes) of
		{ok, DestRoutes} ->
			case add_route_to_list(Dest, Router, Metric, DestRoutes, NotifyPeers) of
				[] -> dict:erase(Dest, Routes);
				NewDestRoutes -> dict:store(Dest, NewDestRoutes, Routes)
			end;
		error ->
			NotifyPeers(Dest, Metric),
			dict:store(Dest, [{Router, Metric}], Routes)
	end.
	
add_route_to_list(Dest, Router, Metric, DestRoutes, NotifyPeers) ->
	add_route_to_list([], Dest, Router, Metric, DestRoutes, NotifyPeers).
add_route_to_list(Acc, _, nil, nil, [], _) ->
	lists:filter(fun
		({_, infinity}) -> false;
		(_) -> true
	end, lists:reverse(Acc));
add_route_to_list(Acc, Dest, nil, nil, [Route|Rest], NotifyPeers) -> add_route_to_list([Route|Acc], Dest, nil, nil, Rest, NotifyPeers);
add_route_to_list(Acc, Dest, Router, Metric, [], NotifyPeers) -> % add route to end of list
	NotifyPeers(Dest, Metric),
	add_route_to_list([{Router, Metric}|Acc], Dest, nil, nil, [], NotifyPeers);
add_route_to_list(Acc, Dest, Router, Metric, [{Router, Metric}|Rest], NotifyPeers) -> % no change
	add_route_to_list([{Router, Metric}|Acc], Dest, nil, nil, Rest, NotifyPeers);
add_route_to_list(Acc, Dest, Router, Metric, [{Router, _}|Rest], NotifyPeers) -> % changed metric
	add_route_to_list(Acc, Dest, Router, Metric, Rest, NotifyPeers);
add_route_to_list(Acc, Dest, Router, Metric, [{OtherRouter, OtherMetric}|Rest], NotifyPeers) when OtherMetric >= Metric -> % insert route
	NotifyPeers(Dest, Metric),
	add_route_to_list([{Router, Metric}|Acc], Dest, nil, nil, [{OtherRouter, OtherMetric}|Rest], NotifyPeers);
add_route_to_list(Acc, Dest, Router, Metric, [OtherRoute|Rest], NotifyPeers) ->
	add_route_to_list([OtherRoute|Acc], Dest, Router, Metric, Rest, NotifyPeers).

% Remove routes that route immediately through RemoveRouter
remove_through(RemoveRouter, Routes, NotifyPeers) ->
	dict:filter(
		fun (_, DestRoutes) -> % drop the empty lists
			DestRoutes =/= []
		end,
		dict:map(fun(Dest, DestRoutes) ->
			remove_through_from_list(RemoveRouter, Dest, DestRoutes, NotifyPeers)
		end, Routes)).
remove_through_from_list(RemoveRouter, Dest, List, NotifyPeers) -> remove_through_from_list([], RemoveRouter, Dest, List, NotifyPeers).
remove_through_from_list(Acc, _, _, [], _) -> lists:reverse(Acc);
remove_through_from_list(Acc, RemoveRouter, Dest, [{RemoveRouter, _}|Rest], NotifyPeers) ->
	NotifyPeers(Dest, infinity),
	remove_through_from_list(Acc, RemoveRouter, Dest, Rest, NotifyPeers);
remove_through_from_list(Acc, RemoveRouter, Dest, [OtherRouter|Rest], NotifyPeers) ->
	remove_through_from_list([OtherRouter|Acc], RemoveRouter, Dest, Rest, NotifyPeers).


