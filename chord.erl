% The chord naming service.
-module(chord).
-behaviour(gen_server).

-export([test/0]).

-export([start_link/3, find_successor/2, get_predecessor/1, get_successor/1, get_fingers/1]).
-export([on_node_join/2, on_node_leave/2]).

-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2, code_change/3]).

-define(FIX_FINGERS_INTERVAL, 5000).
-define(STABILIZE_INTERVAL, 1000).
-define(CHECK_PREDECESSOR_INTERVAL, 5000).
-define(RPC_TIMEOUT, 1500).

test_run_node_1() ->
	{ok, Gateway} = gateway:start_link(),
	gateway:connect(Gateway, {127,0,0,1}, 1234, 1),
	{ok, _} = chord:start_link(Gateway, truth, bootstrap),
	ok.

additional_nodes(0, _, _) -> done;
additional_nodes(Count, Address, Port) ->
	{ok, Gateway} = gateway:start_link(),
	gateway:connect(Gateway, Address, Port, 1),
	{ok, _} = chord:start_link(Gateway, truth, normal),
	additional_nodes(Count - 1, Address, Port).

test_run_node_2() ->
	{ok, Gateway} = gateway:start_link(),
	gateway:listen(Gateway, {127,0,0,1}, 1234),
	{ok, Truth} = chord:start_link(Gateway, truth, normal),
	additional_nodes(30, {127,0,0,1}, 1234),
	
	receive after 4000 -> ok end, % wait for chord to stabilize
	io:format("away we go~n"),
	
	%do_a_bunch(Truth, 100).
	io:format("first count ~p~n", [count_first(1000, Truth)]).
	%ok.
	%try_hashes(Truth).
	

%try_hashes(Server) -> try_hashes(Server, 255).
%try_hashes(Server, 0) -> do_hash(Server, 0);
%try_hashes(Server, N) -> do_hash(Server, N), try_hashes(Server, N-1).

%do_hash(Server, N) ->
%	Hash = <<N, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>,
%	io:format("~p: ~p~n", [N, readable_name(find_successor(Server, Hash))]).

count_first(Count, Truth) ->
	count_first_thing(do_a_bunch(Truth, Count)).
count_first_thing([First|Rest]) ->
	io:format("first thing ~p~n", [readable_name(First)]),
	count_first_thing(0, First, [First|Rest]).
count_first_thing(Acc, _, []) -> Acc;
count_first_thing(Acc, First, [First|Rest]) ->
	count_first_thing(Acc+1, First, Rest);
count_first_thing(Acc, First, [_|Rest]) ->
	count_first_thing(Acc, First, Rest).

do_a_bunch(Chord, Count) -> do_a_bunch(Chord, [], Count).
do_a_bunch(_Chord, Acc, 0) -> Acc;
do_a_bunch(Chord, Acc, N) ->
	Next = find_successor(Chord, crypto:sha(term_to_binary(N))),
	do_a_bunch(Chord, [Next|Acc], N-1).

test_node_pair() ->
	Self = self(),
	One = spawn_link(fun() -> Self ! {one, test_run_node_1()}, receive never -> ok end end),
	Two = spawn_link(fun() -> Self ! {two, test_run_node_2()}, receive never -> ok end end),
	OneResult = receive
		{one, Result1} -> Result1
	end,
	TwoResult = receive
		{two, Result2} -> Result2
	end,
	One ! never,
	Two ! never,
	{OneResult, TwoResult}.

run_test(Test) ->
	Me = self(),
	spawn(fun() ->
		Me ! try
			{ok, Test()}
		catch
			throw:Error -> {throw, Error}
		end,
		exit(shutdown)
	end),
	receive
		{ok, Result} -> Result;
		{throw, Error} -> throw(Error);
		{error, Error} -> erlang:error(Error);
		{exit, Error} -> exit(Error)
	end.

test() ->
	run_test(fun test_node_pair/0).

start_link(Gateway, ServiceName, Bootstrap) ->
	gen_server:start_link(?MODULE, [Gateway, {?MODULE, ServiceName}, Bootstrap], []).

init([Gateway, ServiceName, Bootstrap]) ->
	{ok, _} = timer:send_after(?STABILIZE_INTERVAL, stabilize),
	{ok, _} = timer:send_after(?FIX_FINGERS_INTERVAL, fix_fingers),
	{ok, _} = timer:send_after(?CHECK_PREDECESSOR_INTERVAL, check_predecessor),
	
	Name = gateway:get_name(Gateway),
	Hash = crypto:sha(Name),
	
	% Build the RPC handler function.
	Server = self(),
	RpcHandler = fun
		(call, _, {find_successor, ID}) ->
			find_successor(Server, ID);
		(call, _, get_predecessor) ->
			get_predecessor(Server);
		(call, _, get_successor) ->
			get_successor(Server);
		(cast, From, notify) ->
			gen_server:cast(Server, {notify, From})
	end,
	
	RPC_Call = gateway:call(Gateway, ?RPC_TIMEOUT, ServiceName),
	RPC_Cast = gateway:cast(Gateway, ServiceName),
	RPC = {RPC_Call, RPC_Cast},
	
	Successor = case Bootstrap of
		bootstrap -> Name;
		_ -> join(Gateway, ServiceName, Server, Hash, 0)
	end,
	
	% Must register after joining so we don't just join ourself. This is why you need to start with a bootstrap node.
	ok = gateway:register(Gateway, ServiceName, RpcHandler),
	
	OnNodeAction = fun(_) -> ok end,
	Events = {OnNodeAction, OnNodeAction},
	
	% RPC, MyID, Hash, Predecessor, Successors, NextFinger, Fingers
	{ok, {RPC, Events, Name, Hash, nil, [Successor], 0, array:new()}}.

on_node_join(Server, OnNodeJoin) ->
	gen_server:call(Server, {on_node_join, OnNodeJoin}).

on_node_leave(Server, OnNodeLeave) ->
	gen_server:call(Server, {on_node_leave, OnNodeLeave}).

join(Gateway, ServiceName, Server, Hash, Wait) ->
	case gateway:call_group(Gateway, Wait, ServiceName, {find_successor, Hash}) of
		{ok, Successor} ->
			Successor;
		timeout -> % retry
			join(Gateway, ServiceName, Server, Hash, next_wait(Wait))
	end.

get_predecessor(Server) ->
	gen_server:call(Server, get_predecessor).

get_successor(Server) ->
	gen_server:call(Server, get_successor).

next_wait(0) -> 50; % start at 50 ms
next_wait(Wait) when Wait >= 10000 -> 10000; % max 10 s wait
next_wait(Wait) -> Wait * 2. % double the wait each time (exponential)

find_successor(Server, ID) ->
	case gen_server:call(Server, {find_successor, ID}, ?RPC_TIMEOUT + 5000) of
		{ok, Result} -> Result;
		timeout -> % retry
			find_successor(Server, ID)
	end.
	
get_fingers(Server) ->
	gen_server:call(Server, get_fingers).

readable_name(nil) -> nil;
readable_name(Name) ->
	Hash = crypto:sha(Name),
	<<Readable:8, _/binary>> = Hash,
	Readable.

show_state(_) -> ok.
%show_state({_RPC, MyID, _Hash, Predecessor, [Successor|_], _NextFinger, _Fingers}) ->
%	io:format("~p <- ~p -> ~p~n", [readable_name(Predecessor), readable_name(MyID), readable_name(Successor)]).

handle_cast(Request, State) ->
	case Result = my_handle_cast(Request, State) of
		{noreply, NewState} when NewState =/= State -> show_state(NewState);
		_ -> show_nothing
	end,
	Result.

handle_call(Request, Client, State) ->
	{{RPC_Call, RPC_Cast}, {OnNodeJoin, OnNodeLeave}, MyID, Hash, Predecessor, [Successor|Successors], NextFinger, Fingers} = State,
	case Request of
		{find_successor, ID} ->
			spawn_link(fun() ->
				case key_ranges:in_half_open_range(ID, Hash, crypto:sha(Successor)) of
					true ->
						gen_server:reply(Client, {ok, Successor});
					_ ->
						OtherNode = closest_preceding_node(Successor, Hash, ID, Fingers),
						gen_server:reply(Client, RPC_Call(OtherNode, {find_successor, ID}))
				end
			end),
			{noreply, State};
		
		get_successor ->
			{reply, Successor, State};
		
		get_predecessor ->
			{reply, Predecessor, State};
		
		get_fingers ->
			Result = sets:to_list(sets:from_list([Successor|array:to_list(Fingers)])),
			{reply, Result, State};
		
		{on_node_join, ONJ} ->
			{reply, ok, {{RPC_Call, RPC_Cast}, {ONJ, OnNodeLeave}, MyID, Hash, Predecessor, [Successor|Successors], NextFinger, Fingers}};
		{on_node_leave, ONL} ->
			{reply, ok, {{RPC_Call, RPC_Cast}, {OnNodeJoin, ONL}, MyID, Hash, Predecessor, [Successor|Successors], NextFinger, Fingers}}
	end.

handle_info(Request, State) ->
	{{RPC_Call, RPC_Cast}, _Events, MyID, Hash, Predecessor, [Successor|Successors], NextFinger, Fingers} = State,
	case Request of
		stabilize ->
			Server = self(),
			FingerCount = array:size(Fingers),
			spawn_link(fun() -> stabilize(Server, {RPC_Call, RPC_Cast}, MyID, Hash, [Successor|Successors], FingerCount) end),
			{noreply, State};
		fix_fingers ->
			Server = self(),
			spawn_link(fun() -> fix_fingers(Server, Hash, NextFinger, Fingers) end),
			{noreply, State};
		check_predecessor ->
			Server = self(),
			spawn_link(fun() -> check_predecessor(Server, RPC_Call, Predecessor) end),
			timer:send_after(?CHECK_PREDECESSOR_INTERVAL, check_predecessor),
			{noreply, State}
	end.

my_handle_cast(Request, State) ->
	{RPC, {OnNodeJoin, OnNodeLeave}, MyID, Hash, Predecessor, [Successor|Successors], NextFinger, Fingers} = State,
	case Request of
		{set_successors, NewSuccessors} ->
			timer:send_after(?STABILIZE_INTERVAL, stabilize),
			{noreply, {RPC, {OnNodeJoin, OnNodeLeave}, MyID, Hash, Predecessor, NewSuccessors, NextFinger, Fingers}};
		
		{set_fingers, NewNextFinger, NewFingers} ->
			timer:send_after(?FIX_FINGERS_INTERVAL, fix_fingers),
			{noreply, {RPC, {OnNodeJoin, OnNodeLeave}, MyID, Hash, Predecessor, [Successor|Successors], NewNextFinger, NewFingers}};
		
		unset_predecessor ->
			spawn_link(fun() ->
				OnNodeLeave(Predecessor)
			end),
			{noreply, {RPC, {OnNodeJoin, OnNodeLeave}, MyID, Hash, nil, [Successor|Successors], NextFinger, Fingers}};
			
		{notify, OtherNode} ->
			NewPredecessor = case (Predecessor =:= nil) orelse key_ranges:in_open_range(crypto:sha(OtherNode), crypto:sha(Predecessor), Hash) of
				true ->
					spawn_link(fun() ->
						OnNodeJoin(OtherNode)
					end),
					OtherNode;
				_ ->
					Predecessor
			end,
			{noreply, {RPC, {OnNodeJoin, OnNodeLeave}, MyID, Hash, NewPredecessor, [Successor|Successors], NextFinger, Fingers}}
	end.

closest_preceding_node(Successor, Hash, ID, Fingers) ->
	closest_preceding_node(Successor, Hash, ID, Fingers, array:size(Fingers)).
closest_preceding_node(Successor, _, _, _, 0) -> Successor;
closest_preceding_node(Successor, Hash, ID, Fingers, LastIndex) ->
	Index = LastIndex - 1,
	Finger = array:get(Index, Fingers),
	case key_ranges:in_open_range(crypto:sha(Finger), Hash, ID) of
		true -> Finger;
		_ -> closest_preceding_node(Successor, Hash, ID, Fingers, Index)
	end.

set_successors(Server, NewSuccessors) ->
	gen_server:cast(Server, {set_successors, NewSuccessors}).

stabilize(Server, {RPC_Call, RPC_Cast}, MyID, Hash, [Successor|Successors], FingerCount) ->
	RemainingSuccessors = case RPC_Call(Successor, get_predecessor) of
		{ok, X} -> HashSuccessor = crypto:sha(Successor),
			NewSuccessor = case X =/= nil andalso key_ranges:in_open_range(crypto:sha(X), Hash, HashSuccessor) of
				true ->
					set_successors(Server, [X|Successors]), X;
				_ -> Successor
			end,
			[NewSuccessor|Successors];
		timeout ->
			Successors
	end,
	
	[NotifySuccessor|_] = RemainingSuccessors,
	RPC_Cast(NotifySuccessor, notify),
	
	NewSuccessors = if
		length(RemainingSuccessors) =:= 0 ->
			[MyID];
		length(RemainingSuccessors) < FingerCount ->
			LastSuccessor = lists:last(RemainingSuccessors),
			case RPC_Call(LastSuccessor, get_successor) of
				{ok, AddedSuccessor} ->
					% append AddedSuccessor
					lists:reverse([AddedSuccessor|lists:reverse(RemainingSuccessors)]);
				timeout -> RemainingSuccessors
			end;
		true -> RemainingSuccessors
	end,
	set_successors(Server, NewSuccessors).

fix_fingers(Server, Hash, Index, Fingers) ->
	% FingerHash = Hash + 2 ^ Index
	Bytes = size(Hash),
	Bits = Bytes * 8,
	<<HashNum:Bits>> = Hash,
	FingerHashNum = HashNum + (1 bsl Index),
	FingerHash = <<FingerHashNum:Bits>>,
	
	% NewFinger = successor(FingerHash)
	% NextAction = recurse or wait
	{NextAction, NewFinger} = if
		Index > 0 ->
			LastFinger = array:get(Index - 1, Fingers),
			LastFingerHash = crypto:sha(LastFinger),
			case key_ranges:in_half_open_range(FingerHash, Hash, LastFingerHash) of
				true -> {recurse, LastFinger};
				_ -> {wait, find_successor(Server, FingerHash)}
			end;
		true ->
			{wait, find_successor(Server, FingerHash)}
	end,
	
	NewFingers = array:set(Index, NewFinger, Fingers),
	
	NewIndex = if
		Index >= Bits - 1 -> 0;
		true -> Index + 1
	end,
	
	case NextAction of
		recurse ->
			fix_fingers(Server, Hash, NewIndex, NewFingers);
		wait ->
			gen_server:cast(Server, {set_fingers, NewIndex, NewFingers})
	end.

check_predecessor(Server, RPC_Call, Predecessor) ->
	case ping(RPC_Call, Predecessor) of
		ok -> do_nothing;
		_ -> gen_server:cast(Server, unset_predecessor)
	end.

ping(_, nil) -> nil;
ping(RPC_Call, Name) ->
	Hash = crypto:sha(Name),
	case RPC_Call(Name, {find_successor, Hash}) of
		{ok, Name} -> ok;
		_ -> failed
	end.

terminate(_Reason, _State) ->
	% gateway:unregister(Gateway, ServiceName), if such a thing existed
	ok.

code_change(_Version, State, _Extra) -> {ok, State}.