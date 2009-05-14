% The chord naming service.
-module(chord).
-behaviour(gen_server).

-export([test/0]).
-export([start_link/3, find_successor/2, increment_hash/1]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2, code_change/3]).

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
			throw:Error -> {throw, Error};
			error:Error -> {error, Error};
			exit:Error -> {exit, Error}
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
	{ok, StabilizeTimer} = timer:send_interval(500, stabilize),
	{ok, FixFingerTimer} = timer:send_interval(2000, fix_fingers),
	{ok, CheckPredecessorTimer} = timer:send_interval(5000, check_predecessor),
	Timers = {StabilizeTimer, FixFingerTimer, CheckPredecessorTimer},
	
	Name = gateway:get_name(Gateway),
	Hash = crypto:sha(Name),
	
	% Build the RPC handler function.
	Server = self(),
	RpcHandler = fun
		(call, _, {find_successor, ID}) ->
			find_successor(Server, ID);
		(call, _, get_predecessor) ->
			gen_server:call(Server, get_predecessor);
		(call, _, get_successor) ->
			gen_server:call(Server, get_successor);
		(cast, From, notify) ->
			gen_server:cast(Server, {notify, From})
	end,
	RpcTimeout = 5000,
	
	RPC_Call = gateway:call(Gateway, RpcTimeout, ServiceName),
	RPC_Cast = gateway:cast(Gateway, ServiceName),
	RPC = {RPC_Call, RPC_Cast},
	
	Successor = case Bootstrap of
		bootstrap -> Name;
		_ -> join(Gateway, ServiceName, Server, Hash, 0)
	end,
	
	% Must register after joining so we don't just join ourself. This is why you need to start with a bootstrap node.
	ok = gateway:register(Gateway, ServiceName, RpcHandler),
	
	% RPC, MyID, Hash, Predecessor, Successors, NextFinger, Fingers
	{ok, {RPC, Name, Hash, nil, [Successor], 0, array:new(), Timers}}.

join(Gateway, ServiceName, Server, Hash, Wait) ->
	case gateway:call_group(Gateway, Wait, ServiceName, {find_successor, Hash}) of
		{ok, Successor} ->
			Successor;
		timeout -> % retry
			join(Gateway, ServiceName, Server, Hash, next_wait(Wait))
	end.

next_wait(0) -> 50; % start at 50 ms
next_wait(Wait) when Wait >= 10000 -> 10000; % max 10 s wait
next_wait(Wait) -> Wait * 2. % double the wait each time (exponential)

find_successor(Server, ID) ->
	gen_server:call(Server, {find_successor, ID}).

readable_name(nil) -> nil;
readable_name(Name) ->
	Hash = crypto:sha(Name),
	<<Readable:8, _/binary>> = Hash,
	Readable.

show_state(_) -> ok.
%show_state({_RPC, MyID, _Hash, Predecessor, [Successor|_], _NextFinger, _Fingers, _Timers}) ->
%	io:format("~p <- ~p -> ~p~n", [readable_name(Predecessor), readable_name(MyID), readable_name(Successor)]).

handle_cast(Request, State) ->
	case Result = my_handle_cast(Request, State) of
		{noreply, NewState} when NewState =/= State -> show_state(NewState);
		_ -> show_nothing
	end,
	Result.

handle_call(Request, Client, State) ->
	{{RPC_Call, _RPC_Cast}, _MyID, Hash, Predecessor, [Successor|_Successors], _NextFinger, Fingers, _Timers} = State,
	case Request of
		{find_successor, ID} ->
			Server = self(),
			spawn_link(fun() ->
				case in_half_open_range(ID, Hash, crypto:sha(Successor)) of
					true ->
						gen_server:reply(Client, Successor);
					_ ->
						OtherNode = closest_preceding_node(Successor, Hash, ID, Fingers),
						case RPC_Call(OtherNode, {find_successor, ID}) of
							{ok, Result} -> gen_server:reply(Client, Result);
							
							timeout -> % retry
								receive after 1000 -> ok end,
								gen_server:reply(Client, find_successor(Server, ID))						
						end
				end
			end),
			{noreply, State};
		
		get_successor ->
			{reply, Successor, State};
		
		get_predecessor ->
			{reply, Predecessor, State}
	end.

handle_info(Request, State) ->
	{{RPC_Call, RPC_Cast}, MyID, Hash, Predecessor, [Successor|Successors], NextFinger, Fingers, Timers} = State,
	case Request of
		stabilize ->
			%io:format("stabilize~n"),
			Server = self(),
			FingerCount = array:size(Fingers),
			spawn_link(fun() -> stabilize(Server, {RPC_Call, RPC_Cast}, MyID, Hash, [Successor|Successors], FingerCount) end),
			{noreply, State};
		fix_fingers ->
			case NextFinger of
				block -> {noreply, State};
				_ ->
					Server = self(),
					spawn_link(fun() -> fix_fingers(Server, Hash, NextFinger) end),
					{noreply, {{RPC_Call, RPC_Cast}, MyID, Hash, Predecessor, [Successor|Successors], block, Fingers, Timers}}
			end;
		check_predecessor ->
			Server = self(),
			spawn_link(fun() -> check_predecessor(Server, RPC_Call, Predecessor) end),
			{noreply, State}
	end.

my_handle_cast(Request, State) ->
	{RPC, MyID, Hash, Predecessor, [Successor|Successors], NextFinger, Fingers, Timers} = State,
	case Request of
		{set_successors, NewSuccessors} ->
			{noreply, {RPC, MyID, Hash, Predecessor, NewSuccessors, NextFinger, Fingers, Timers}};
		{set_finger, NewNextFinger, Index, NewFinger} ->
			NextFinger = block, % verify - should alwaays be the case
			{noreply, {RPC, MyID, Hash, Predecessor, [Successor|Successors], NewNextFinger, array:set(Index, NewFinger, Fingers), Timers}};
		unset_predecessor ->
			{noreply, {RPC, MyID, Hash, nil, [Successor|Successors], NextFinger, Fingers, Timers}};
		{notify, OtherNode} ->
			NewPredecessor = case (Predecessor =:= nil) orelse in_open_range(crypto:sha(OtherNode), crypto:sha(Predecessor), Hash) of
				true ->
					OtherNode;
				_ ->
					Predecessor
			end,
			{noreply, {RPC, MyID, Hash, NewPredecessor, [Successor|Successors], NextFinger, Fingers, Timers}}
	end.

closest_preceding_node(Successor, Hash, ID, Fingers) ->
	closest_preceding_node(Successor, Hash, ID, Fingers, array:size(Fingers)).
closest_preceding_node(Successor, _, _, _, 0) -> Successor;
closest_preceding_node(Successor, Hash, ID, Fingers, LastIndex) ->
	Index = LastIndex - 1,
	Finger = array:get(Index, Fingers),
	case in_open_range(crypto:sha(Finger), Hash, ID) of
		true -> Finger;
		_ -> closest_preceding_node(Successor, Hash, ID, Fingers, Index)
	end.

% is Value in (From, To)
in_open_range(nil, _, _) -> false;
in_open_range(Value, From, To) ->
	if
		(From < To) ->
			(Value > From) and (Value < To);
		(To < From) ->
			(Value > From) or (Value < To);
		true -> true
	end.

% is Value in (From, To]
in_half_open_range(nil, _, _) -> false;
in_half_open_range(Value, From, To) ->
	if
		(From < To) ->
			(Value > From) and (Value =< To);
		(To < From) ->
			(Value > From) or (Value =< To);
		true -> true
	end.

set_successors(Server, NewSuccessors) ->
	gen_server:cast(Server, {set_successors, NewSuccessors}).

stabilize(Server, {RPC_Call, RPC_Cast}, MyID, Hash, [Successor|Successors], FingerCount) ->
	RemainingSuccessors = case RPC_Call(Successor, get_predecessor) of
		{ok, X} -> HashSuccessor = crypto:sha(Successor),
			NewSuccessor = case X =/= nil andalso in_open_range(crypto:sha(X), Hash, HashSuccessor) of
				true ->
					set_successors(Server, [X|Successors]), X;
				_ -> Successor
			end,
			RPC_Cast(NewSuccessor, notify),
			[NewSuccessor|Successors];
		timeout ->
			Successors
	end,
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

fix_fingers(Server, Hash, NextFinger) ->
	M = size(Hash),
	FingerHash = add_hash(Hash, hash_two_exp_n(M, NextFinger)),
	NewFinger = find_successor(Server, FingerHash),
	NewNextFinger = NextFinger + 1,
	ClampedNewNextFinger = if NewNextFinger >= M -> 0; true -> NewNextFinger end,
	gen_server:cast(Server, {set_finger, ClampedNewNextFinger, NextFinger, NewFinger}).

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

hash_two_exp_n(SizeInBytes, N) ->
	Bits = SizeInBytes * 8,
	<<0:(Bits-N-1), 1:1, 0:N>>.

add_hash(A, B) ->
	list_to_binary(
	  add_hash([],
			   lists:reverse(binary_to_list(A)),
			   lists:reverse(binary_to_list(B)), 0)).
add_hash(Acc, [], [], _) -> Acc;
add_hash(Acc, [ByteA|RestA], [ByteB|RestB], Carry) ->
	Sum = ByteA + ByteB + Carry,
	NewByte = Sum rem 256,
	NewCarry = Sum div 256,
	add_hash([NewByte|Acc], RestA, RestB, NewCarry).
	
increment_hash(Hash) ->
	Size = size(Hash) * 8,
	One = <<1:Size>>,
	add_hash(Hash, One).

terminate(_Reason, State) ->
	{_RPC, _MyID, _Hash, _Predecessor, _Successors, _NextFinger, _Fingers, Timers} = State,
	{StabilizeTimer, FixFingerTimer, CheckPredecessorTimer} = Timers,
	{ok, cancel} = timer:cancel(StabilizeTimer),
	{ok, cancel} = timer:cancel(FixFingerTimer),
	{ok, cancel} = timer:cancel(CheckPredecessorTimer),
	% gateway:unregister(Gateway, ServiceName), if such a thing existed
	ok.

code_change(_Version, State, _Extra) -> {ok, State}.