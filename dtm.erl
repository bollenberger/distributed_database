% Distributed Transaction Manager
% Uses the (local) Transaction Manager (tm), and communicates via the gateway to implement modififed 3PC.
% The first phase is implicit based upon the previous transaction operations completing from the
% point of view of the transaction coordinator. This reduces latency at commit time.
-module(dtm).
-behaviour(gen_server).

-export([test/0]).
-export([start_link/4, transaction/2]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2, code_change/3]).

workload(DTM) ->
	Times = 100,
	A = 100, B = -100,
	dtm:transaction(DTM, fun(_, Put, _) ->
		Put(a, A), Put(b, B)
	end),
	
	Parent = self(),
	spawn_link(fun() -> workload(DTM, 1, Times), Parent ! done end),
	spawn_link(fun() -> workload(DTM, -1, Times), Parent ! done end),
	wait(2),

	dtm:transaction(DTM, fun(Get, _, Delete) ->
		{ok, A} = Get(a),
		{ok, B} = Get(b),
		Delete(a), Delete(b)
	end),
	dtm:transaction(DTM, fun(Get, _, _) ->
		no_value = Get(a),
		no_value = Get(b)
	end).
	
workload(_, _, 0) -> ok;
workload(DTM, Increment, Times) ->
	dtm:transaction(DTM, fun(Get, Put, _) ->
		{ok, A} = Get(a),
		{ok, B} = Get(b),
		io:format("~p ~p ~p~n", [A, B, Increment]),
		Put(a, A + Increment),
		Put(b, B - Increment)
	end),
	workload(DTM, Increment, Times - 1).
	
node_a(Main) ->
	{ok, Gateway} = gateway:start_link(),
	gateway:connect(Gateway, {127,0,0,1}, 1234, 1),
	{ok, DTM} = dtm:start_link_bootstrap(Gateway, truth, 1),
	receive after 2000 -> ok end,
	
	% provide a work load
	workload(DTM),

	Main ! done.

node_b(_Main) ->
	{ok, Gateway} = gateway:start_link(),
	gateway:listen(Gateway, {127,0,0,1}, 1234),
	gateway:connect(Gateway, {127,0,0,1}, 1235, 1),
	{ok, _} = dtm:start_link(Gateway, truth, 1),
	wait(1).

node_c(_Main) ->
	{ok, Gateway} = gateway:start_link(),
	gateway:listen(Gateway, {127,0,0,1}, 1235),
	{ok, _} = dtm:start_link(Gateway, truth, 1),
	wait(1).

wait(0) -> ok;
wait(Times) ->
	receive _ -> wait(Times - 1) end.

test_consistency() ->
	Main  = self(),
	C = spawn_link(fun() -> node_c(Main) end),
	B = spawn_link(fun() -> node_b(Main) end),
	_A = spawn_link(fun() -> node_a(Main) end),
	wait(1), B ! done, C ! done,
	ok.
	
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
	run_test(fun test_consistency/0).

% Takes an Operation/3 function that takes Get, Put, and Delete
% operations as parameters.
% If Operation throws an exception, the transaction will be aborted and the exception will be thrown by transaction().
% If Operation returns abort, the transaction is aborted and transaction returns {abort, abort}.
% Any other return value of Operation will cause the transaction to be committed.
% If required, the Operation may be retried automatically, so it must not have any side effects.
% On successful commit or abort, transaction() returns the return value of Operation.
transaction(DTM, Operation) -> transaction(DTM, Operation, 0, 0).
transaction(DTM, Operation, Clock, Wait) ->
	receive after Wait -> ok end,
	Tid = begin_transaction(DTM, Clock),
	Get = fun(Key) ->
		XX = get(DTM, Tid, Key),
		%io:format("~p: Get(~p) = ~p~n", [Tid, Key, XX]),
		case XX of
			{retry, NewClock} -> throw({retry, NewClock});
			no_value -> no_value;
			{ok, X} -> {ok, X}
		end
	end,
	Put = fun(Key, Value) ->
		XX = put(DTM, Tid, Key, Value),
		%io:format("~p: Put(~p, ~p) = ~p~n", [Tid, Key, Value, XX]),
		case XX of
			{retry, NewClock} -> throw({retry, NewClock});
			ok -> ok
		end
	end,
	Delete = fun(Key) ->
		case delete(DTM, Tid, Key) of
			{retry, NewClock} -> throw({retry, NewClock});
			ok -> ok
		end
	end,
	
	% abort, {ok, Output}, {retry, NewClock}, or aborts and throws error
	Result = try
		case Operation(Get, Put, Delete) of
			abort ->
				ok = abort(DTM, Tid),
				abort;
			Output ->
				case commit(DTM, Tid) of
					ok -> Output;
					abort -> throw({retry, 0})
				end
		end
	catch
		throw:{retry, NewClock} ->
			abort(DTM, Tid), {retry, NewClock};
		throw:Error ->
			abort(DTM, Tid), throw(Error);
		exit:Error ->
			abort(DTM, Tid), exit(Error);
		error:Error ->
			abort(DTM, Tid), erlang:error(Error)
	end,

	case Result of
		abort -> abort;
		{retry, NewClock2} ->
			%io:format("retry ~p ~p~n", [Tid, NewClock2]),
			transaction(DTM, Operation, NewClock2, next_wait(Wait));
		_ ->
			%io:format("done transaction ~p ~p~n", [Tid, Result]),
			Result
	end.

next_wait(0) -> 50; % start at 50 ms
next_wait(Wait) when Wait >= 10000 -> 10000; % max 10 s wait
next_wait(Wait) -> Wait * 2. % double the wait each time (exponential)

start_link(Gateway, ServiceName, Replication, Bootstrap) when Replication > 0 ->
	gen_server:start_link(?MODULE, [Gateway, ServiceName, Replication, Bootstrap], []).

begin_transaction(DTM, Clock) ->
	gen_server:call(DTM, {begin_transaction, Clock}).

% returns {ok, Value}, no_value, {retry, Clock}
get(DTM, Tid, Key) ->
	case gen_server:call(DTM, {get, Tid, Key}) of
		{retry, Clock} -> throw({retry, Clock});
		Result -> Result
	end.

% returns ok, {retry, Clock}
put(DTM, Tid, Key, Value) ->
	case gen_server:call(DTM, {put, Tid, Key, Value}) of
		{retry, Clock} -> throw({retry, Clock});
		Result -> Result
	end.

% returns ok, {retry, Clock}
delete(DTM, Tid, Key) ->
	case gen_server:call(DTM, {delete, Tid, Key}) of
		{retry, Clock} -> throw({retry, Clock});
		Result -> Result
	end.

% returns ok or abort
commit(DTM, Tid) ->
	gen_server:call(DTM, {commit, Tid}).

% returns ok
abort(DTM, Tid) ->
	gen_server:call(DTM, {abort, Tid}).

service_name(ServiceName, Replication) ->
	{?MODULE, ServiceName, Replication}.
init([Gateway, SN, Replication, Bootstrap]) ->
	ServiceName = service_name(SN, Replication),
	
	{ok, Chord} = chord:start_link(Gateway, ServiceName, Bootstrap),
	
	{ok, TM} = tm:start_link(),
	Name = gateway:get_name(Gateway),
	Clock = 0,
	Transactions = dict:new(),
	
	RpcHandler = fun
		(call, _, {get, Tid, Key}) ->
			tm:get(TM, Tid, Key);
		(call, _, {put, Tid, Key, Value}) ->
			tm:put(TM, Tid, Key, Value);
		(call, _, {delete, Tid, Key}) ->
			tm:delete(TM, Tid, Key);
		(cast, _, {abort, Tid}) ->
			tm:abort(TM, Tid);
		(call, _, {prepare, Tid}) ->
			tm:prepare(TM, Tid);
		(cast, _, {commit, Tid}) ->
			tm:commit(TM, Tid)
	end,
	RpcTimeout = 5000,
	
	ok = gateway:register(Gateway, ServiceName, RpcHandler),
	RPC_Call = gateway:call(Gateway, RpcTimeout, ServiceName),
	RPC_Cast = gateway:cast(Gateway, ServiceName),
	
	{ok, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, Transactions}}.

% Make a numeric transaction ID representing the tuple {Clock, Name}
% This is necessary because the transaction manager only accepts
% numeric transaction IDs, which must be unique. They are sequenced
% by the high order clock and uniqueness is guaranteed by the low order
% node name.
make_tid(Clock, Name) ->
	Size = size(Name) * 8,
	<<NameNum:Size>> = Name,
	NameNum bor (Clock bsl Size).

% Given a known Tid (timestamp), return the new clock to use for future
% transactions.
new_clock(Clock, Name, Tid) ->
	Size = size(Name) * 8,
	erlang:max(Clock + 1, (Tid div (1 bsl Size)) + 1).

% Get a set of node names that should own Key. - Maybe possible to optimize this. Call the successor of the key directly to ask it for its successors. But this'll do for now.
find_nodes(Chord, Key, Replication) ->
	find_nodes([], Chord, crypto:sha(term_to_binary(Key)), Replication).
find_nodes(Acc, _, _, 0) -> sets:from_list(Acc);
find_nodes(Acc, Chord, Key, Replication) ->
	Node = chord:find_successor(Chord, Key),
	<<HashNum:160>> = crypto:sha(Node),
	Hash = <<(HashNum+1):160>>,
	find_nodes([Node|Acc], Chord, Hash, Replication - 1).

add_nodes_to_transactions(Transactions, Tid, Nodes) ->
	dict:update(Tid, fun(OldNodes) -> sets:union(OldNodes, Nodes) end, Transactions).

for_each_node_reduce(Acc, _, 0) -> Acc;
for_each_node_reduce(Acc, Reduce, Count) ->
	receive
		Value -> for_each_node_reduce(Reduce(Value, Acc), Reduce, Count - 1)
	end.
for_each_node(NodeSet, Map, Reduce) ->
	Nodes = sets:to_list(NodeSet),
	NodeCount = length(Nodes),
	ReducePid = self(),
	lists:map(fun(Node) ->
		spawn_link(fun() ->
			ReducePid ! Map(Node)
		end)
	end, Nodes),
	for_each_node_reduce(no_reply, Reduce, NodeCount).

reduce_get_results(Result, Acc) ->
	case Acc of
		{ok, _Value, Created} ->
			case Result of
				timeout -> Acc;
				{ok, {ok, Value2, Created2}} when Created2 > Created ->
					{ok, Value2, Created2};
				{ok, {no_value, Deleted}} when Deleted > Created ->
					{no_value, Deleted};
				_ -> Acc
			end;
		{no_value, Deleted} ->
			case Result of
				timeout -> Acc;
				{ok, {ok, Value, Created}} when Created > Deleted ->
					{ok, Value, Created};
				{ok, {no_value, Deleted2}} when Deleted2 > Deleted ->
					{no_value, Deleted2};
				_ -> Acc
			end;
		{retry, Clock} ->
			case Result of
				timeout -> Acc;
				{ok, {ok, Value, Created}} ->
					{ok, Value, Created};
				{ok, {no_value, Deleted}} ->
					{no_value, Deleted};
				{ok, {retry, Clock2}} when Clock2 > Clock -> {retry, Clock2};
				_ -> Acc
			end;
		no_reply ->
			case Result of
				timeout -> Acc;
				{ok, X} -> X
			end
	end.

reduce_put_results(Result, Acc) ->
	case Acc of
		ok ->
			case Result of
				{ok, {retry, Clock}} -> {retry, Clock};
				_ -> Acc
			end;
		{retry, Clock} ->
			case Result of
				{ok, {retry, Clock2}} when Clock2 > Clock ->
					{retry, Clock2};
				_ -> Acc
			end;
		no_reply ->
			case Result of
				timeout -> Acc;
				{ok, X} -> X
			end
	end.

reduce_delete_results(Result, Acc) -> reduce_put_results(Result, Acc).

reduce_prepare_results(Result, Acc) ->
	case Acc of
		ok ->
			case Result of
				timeout -> abort;
				{ok, ok} -> ok
			end;
		abort -> abort;
		no_reply ->
			case Result of
				timeout -> abort;
				{ok, ok} -> ok
			end
	end.

handle_call(Request, Client, State) ->
	{{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, Transactions} = State,
	case Request of
		{begin_transaction, MinimumTid} ->
			NewClock = new_clock(Clock, Name, MinimumTid),
			Tid = make_tid(NewClock, Name),
			{reply, Tid, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, NewClock, dict:store(Tid, sets:new(), Transactions)}};

		{get, Tid, Key} ->
			Nodes = find_nodes(Chord, Key, Replication),
			NewTransactions = add_nodes_to_transactions(Transactions, Tid, Nodes),
			spawn_link(fun() ->
				Result = for_each_node(Nodes,
					fun(Node) -> RPC_Call(Node, {get, Tid, Key}) end,
					fun reduce_get_results/2),
				EndResult = case Result of
					{ok, Value, _} -> {ok, Value};
					{no_value, _} -> no_value;
					no_reply -> {retry, 0};
					{retry, NewClock} -> {retry, NewClock}
				end,
				gen_server:reply(Client, EndResult)
			end),
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, NewTransactions}};

		{put, Tid, Key, Value} ->
			Nodes = find_nodes(Chord, Key, Replication),
			NewTransactions = add_nodes_to_transactions(Transactions, Tid, Nodes),
			spawn_link(fun() ->
				Result = for_each_node(Nodes,
					fun(Node) -> RPC_Call(Node, {put, Tid, Key, Value}) end,
					fun reduce_put_results/2),
				EndResult = case Result of
					ok -> ok;
					{retry, NewClock} -> {retry, NewClock};
					no_reply -> {retry, 0}
				end,
				gen_server:reply(Client, EndResult)
			end),
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, NewTransactions}};

		{delete, Tid, Key} ->
			Nodes = find_nodes(Chord, Key, Replication),
			NewTransactions = add_nodes_to_transactions(Transactions, Tid, Nodes),
			spawn_link(fun() ->
				Result = for_each_node(Nodes,
					fun(Node) ->
						RPC_Call(Node, {delete, Tid, Key})
					end,
					fun reduce_delete_results/2),
				EndResult = case Result of
					ok -> ok;
					{retry, NewClock} -> {retry, NewClock};
					no_reply -> {retry, 0}
				end,
				gen_server:reply(Client, EndResult)
			end),
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, NewTransactions}};

		{commit, Tid} ->
			% If the transaction coordinator gets to commit,
			% then we know that the constituent operations of the
			% transactions have already succeeded. So the cohorts may fail, but won't abort
			% For this reason, we can get away with a two phase "3 phase commit".
			% Just skip the first phase, since it's implicit in the completion
			% of the previous get, put and delete operations.
			Nodes = dict:fetch(Tid, Transactions),
			spawn_link(fun() ->
				PrepareResult = for_each_node(Nodes,
					fun(Node) -> RPC_Call(Node, {prepare, Tid}) end,
					fun reduce_prepare_results/2),
				
				Result = case PrepareResult of
					no_reply ->
						do_abort(RPC_Cast, Nodes, Tid),
						abort;
					abort ->
						do_abort(RPC_Cast, Nodes, Tid),
						abort;
					ok ->
						for_each_node(Nodes,
							fun(Node) -> RPC_Cast(Node, {commit, Tid}) end,
							fun(_, _) -> ok end),
						ok
				end,

				gen_server:reply(Client, Result)
			end),
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, dict:erase(Tid, Transactions)}};
		{abort, Tid} ->
			Nodes = dict:fetch(Tid, Transactions),
			spawn_link(fun() ->
				do_abort(RPC_Cast, Nodes, Tid),
				gen_server:reply(Client, ok)
			end),
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, dict:erase(Tid, Transactions)}}
	end.

do_abort(RPC_Cast, Nodes, Tid) ->
	for_each_node(Nodes,
		fun(Node) -> RPC_Cast(Node, {abort, Tid}) end,
		fun(_, _) -> ok end).

handle_cast(_Request, State) ->
	{noreply, State}.

handle_info(_Request, State) ->
	{noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_Version, State, _Extra) -> {ok, State}.