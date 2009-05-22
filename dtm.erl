% Distributed Transaction Manager
% Uses the (local) Transaction Manager (tm), and communicates via the gateway to implement modififed 3PC.
% The first phase is implicit based upon the previous transaction operations completing from the
% point of view of the transaction coordinator. This reduces latency at commit time.
-module(dtm).
-behaviour(gen_server).

-export([test/0]).
-export([start_link/4, transaction/3, transaction/4]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2, code_change/3]).

-define(KEEP_ALIVE_INTERVAL, 10000).
-define(GC_INTERVAL_MULTIPLE, 6).

workload(DTM) ->
	Times = 100,
	%A = 100, B = -100,
	%transaction(DTM, fun(_, Put, _) ->
	%	Put(a, A), Put(b, B)
	%end),
	
	Parent = self(),
	spawn_link(fun() -> workload(DTM, 1, Times), Parent ! done end),
	spawn_link(fun() -> workload(DTM, -1, Times), Parent ! done end),
	wait(2),

	%transaction(DTM, fun(Get, _, Delete) ->
	%	{ok, A} = Get(a),
	%	{ok, B} = Get(b),
	%	Delete(a), Delete(b)
	%end),
	%transaction(DTM, fun(Get, _, _) ->
	%	no_value = Get(a),
	%	no_value = Get(b)
	%end).
	ok.
	
workload(_, _, 0) -> ok;
workload(DTM, Increment, Times) ->
	%transaction(DTM, fun(Get, Put, _) ->
	%	{ok, A} = Get(a),
	%	{ok, B} = Get(b),
	%	io:format("~p ~p ~p~n", [A, B, Increment]),
	%	Put(a, A + Increment),
	%	Put(b, B - Increment)
	%end),
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
	run_test(fun test_consistency/0).

% RPC transaction stub for pure clients of DTM.
% returns one of:
% ok
% {ok, Result}
% {abort, Reason}
% {timeout, TimedoutOperation}
% or throws any error thrown by Operation, unless it is {retry, _} or transaction_timeout
% in which case it will have the associated effect. You should not throw those things from
% Operation in general.
transaction(Gateway, ServiceName, RpcTimeout) -> % curry the transaction stub to only require an operation
	fun(Operation) ->
		transaction(Gateway, ServiceName, RpcTimeout, Operation)
	end.
transaction(Gateway, ServiceName, RpcTimeout, Operation) ->
	transaction(Gateway, ServiceName, RpcTimeout, Operation, 0, 0).
transaction(Gateway, SN, RpcTimeout, Operation, Clock, Wait) ->
	receive after Wait -> ok end,
	ServiceName = {?MODULE, SN},
	
	case gateway:call_group(Gateway, RpcTimeout, ServiceName, {begin_transaction, Clock, RpcTimeout}) of
		timeout ->
			{timeout, begin_transaction};
		{ok, {Tid, PeerName}} ->
			Call = gateway:call(Gateway, RpcTimeout, ServiceName, PeerName),
			Cast = gateway:cast(Gateway, ServiceName, PeerName),
	
			Get = fun(Key) ->
				case Call({get, Tid, Key, RpcTimeout}) of
					{ok, {ok, X}} -> {ok, X};
					{ok, no_value} -> no_value;
					{ok, {retry, NewClock}} -> throw({retry, NewClock});
					timeout -> throw({timeout, {get, Key}})
				end
			end,
			Put = fun(Key, Value) ->
				case Call({put, Tid, Key, Value, RpcTimeout}) of
					{ok, ok} -> ok;
					{ok, {retry, NewClock}} -> throw({retry, NewClock});
					timeout -> throw({timeout, {put, Key}})
				end
			end,
			Delete = fun(Key) ->
				case Call({delete, Tid, Key, RpcTimeout}) of
					{ok, ok} -> ok;
					{ok, {retry, NewClock}} -> throw({retry, NewClock});
					timeout -> throw({timeout, {delete, Key}})
				end
			end,
	
			Result = try
				% case may return:
				% {ok, Result}
				% {abort, Reason}
				% or throw:
				% {retry, 0}
				% transaction_timeout
				case Operation(Get, Put, Delete) of
					ok ->
						case Call({commit, Tid, RpcTimeout}) of
							{ok, ok} -> {ok, ok};
							{ok, abort} -> throw({retry, 0});
							timeout -> throw({timeout, commit_transaction})
						end;
					{ok, Output} ->
						case Call({commit, Tid, RpcTimeout}) of
							{ok, ok} -> {ok, Output};
							{ok, abort} -> throw({retry, 0});
							timeout -> throw({timeout, commit_transaction})
						end;
				
					Reason ->
						Cast({abort, Tid}),
						{abort, Reason}
				end
			catch
				throw:{retry, NewClock} ->
					Cast({abort, Tid}), {retry, NewClock};
				throw:{timeout, TimeoutOperation} ->
					Cast({abort, Tid}), {timeout, TimeoutOperation};
				throw:Error ->
					Cast({abort, Tid}), throw(Error)
				%exit:Error ->
				%	Cast({abort, Tid}), exit(Error);
				%error:Error ->
				%	Cast({abort, Tid}), erlang:error(Error)
			end,
	
			case Result of
				{ok, _} -> Result;
				{abort, _} -> Result;
				{timeout, _} -> Result;
				{retry, NewClock2} ->
					transaction(Gateway, SN, RpcTimeout, Operation,
						NewClock2, next_wait(Wait))
			end
	end.

next_wait(0) -> 50; % start at 50 ms
next_wait(Wait) when Wait >= 10000 -> 10000; % max 10 s wait
next_wait(Wait) -> Wait * 2. % double the wait each time (exponential)

start_link(Gateway, SN, Replication, Bootstrap) when Replication > 0 ->
	gen_server:start_link(?MODULE, [Gateway, SN, Replication, Bootstrap], []).

begin_transaction(DTM, Clock, Timeout) ->
	gen_server:call(DTM, {begin_transaction, Clock}, Timeout + 5000).

% returns {ok, Value}, no_value, {retry, Clock}
get(DTM, Tid, Key, Timeout) ->
	gen_server:call(DTM, {get, Tid, Key}, Timeout + 5000).

% returns ok, {retry, Clock}
put(DTM, Tid, Key, Value, Timeout) ->
	gen_server:call(DTM, {put, Tid, Key, Value}, Timeout + 5000).

% returns ok, {retry, Clock}
delete(DTM, Tid, Key, Timeout) ->
	gen_server:call(DTM, {delete, Tid, Key}, Timeout + 5000).

% returns ok or abort
commit(DTM, Tid, Timeout) ->
	gen_server:call(DTM, {commit, Tid}, Timeout + 5000).

% returns ok
abort(DTM, Tid) ->
	gen_server:call(DTM, {abort, Tid}).

init([Gateway, SN, Replication, Bootstrap]) ->
	{ok, _} = timer:send_after(?KEEP_ALIVE_INTERVAL, keep_alive),
	{ok, _} = timer:send_after(?GC_INTERVAL_MULTIPLE * ?KEEP_ALIVE_INTERVAL, gc),

	ServiceName = {?MODULE, SN},
	DTM = self(),
	{ok, TM} = tm:start_link(),
	Name = gateway:get_name(Gateway),
	Hash = crypto:sha(Name),
	Clock = 0,
	Transactions = dict:new(),
	KeyRanges = [],
	{ok, Chord} = chord:start_link(Gateway, ServiceName, Bootstrap),
	
	% On node join, re-replicate all keys, since any may need to be replicated to the new node
	chord:on_node_join(Chord, fun(_) ->
		tm:for_each_key(TM, fun(Key) ->
			reify(Gateway, SN, Key)
		end)
	end),
	
	% On node leave, re-replicate all keys not in (P, N], since those belong to this node
	chord:on_node_leave(Chord, fun(Node) ->
		tm:for_each_key(TM, fun(Key) ->
			case key_ranges:in_half_open_range(crypto:sha(term_to_binary(Key)), Node, Hash) of
				true -> skip;
				_ -> reify(Gateway, SN, Key)
			end
		end)
	end),
	
	
	TransactionRpcHandler = fun
		(call, _, {begin_transaction, MinClock, Timeout}) ->
			{begin_transaction(DTM, MinClock, Timeout), Name};
		(call, _, {get, Tid, Key, Timeout}) ->
			get(DTM, Tid, Key, Timeout);
		(call, _, {put, Tid, Key, Value, Timeout}) ->
			put(DTM, Tid, Key, Value, Timeout);
		(call, _, {delete, Tid, Key, Timeout}) ->
			delete(DTM, Tid, Key, Timeout);
		(cast, _, {abort, Tid}) ->
			abort(DTM, Tid);
		(call, _, {commit, Tid, Timeout}) ->
			commit(DTM, Tid, Timeout)
	end,
	
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
			sync_clock(DTM, Tid),
			tm:commit(TM, Tid);
		
		(cast, _, {keep_alive, From, To, R}) ->
			keep_alive(DTM, From, To, R)
	end,
	RpcTimeout = 4000,
	
	ok = gateway:register(Gateway, ServiceName, TransactionRpcHandler),
	InternalServiceName = {?MODULE, internal, ServiceName},
	ok = gateway:register(Gateway, InternalServiceName, RpcHandler),
	RPC_Call = gateway:call(Gateway, RpcTimeout, InternalServiceName),
	RPC_Cast = gateway:cast(Gateway, InternalServiceName),
	
	{ok, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, Transactions, KeyRanges}}.

% re-replicate a key: Put(Key, Get(Key))
reify(Gateway, ServiceName, Key) ->
	transaction(Gateway, ServiceName, 5000, fun(Get, Put, Delete) ->
		case Get(Key) of
			no_value ->
				ok = Delete(Key);
			{ok, Value} ->
				ok = Put(Key, Value)
		end
	end).
	
keep_alive(DTM, From, To, Replication) ->
	gen_server:cast(DTM, {keep_alive, From, To, Replication}).

% Make a numeric transaction ID representing the tuple {Clock, Name}
% This is necessary because the transaction manager only accepts
% numeric transaction IDs, which must be unique. They are sequenced
% by the high order clock and uniqueness is guaranteed by the low order
% node name.
make_tid(Clock, Name) ->
	Size = size(Name) * 8,
	<<NameNum:Size>> = Name,
	NameNum bor (Clock bsl Size).

% Sync the local clock up to a given Tid.
sync_clock(DTM, Tid) ->
	gen_server:call(DTM, {sync_clock, Tid}).


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
				{ok, {retry, Retry}} when Retry > Created ->
					{retry, Retry};
				_ -> Acc
			end;
		{no_value, Deleted} ->
			case Result of
				timeout -> Acc;
				{ok, {ok, Value, Created}} when Created > Deleted ->
					{ok, Value, Created};
				{ok, {no_value, Deleted2}} when Deleted2 > Deleted ->
					{no_value, Deleted2};
				{ok, {retry, Retry}} when Retry > Deleted ->
					{retry, Retry};
				_ -> Acc
			end;
		{retry, Retry} ->
			case Result of
				timeout -> Acc;
				{ok, {ok, Value, Created}} when Created > Retry ->
					{ok, Value, Created};
				{ok, {no_value, Deleted}} when Deleted > Retry ->
					{no_value, Deleted};
				{ok, {retry, Retry2}} when Retry2 > Retry -> {retry, Retry2};
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
	{{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, Transactions, KeyRanges} = State,
	case Request of
		{sync_clock, Tid} ->
			NewClock = new_clock(Clock, Name, Tid),
			{reply, ok, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, NewClock, Transactions, KeyRanges}};
		
		{begin_transaction, MinimumTid} ->
			NewClock = new_clock(Clock, Name, MinimumTid),
			Tid = make_tid(NewClock, Name),
			{reply, Tid, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, NewClock, dict:store(Tid, sets:new(), Transactions), KeyRanges}};

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
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, NewTransactions, KeyRanges}};

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
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, NewTransactions, KeyRanges}};

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
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, NewTransactions, KeyRanges}};

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
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, dict:erase(Tid, Transactions), KeyRanges}};
		{abort, Tid} ->
			case dict:find(Tid, Transactions) of
				error -> do_nothing; % commit failed
				{ok, Nodes} ->
					spawn_link(fun() ->
						do_abort(RPC_Cast, Nodes, Tid),
						gen_server:reply(Client, ok)
					end)
			end,
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, dict:erase(Tid, Transactions), KeyRanges}}
	end.

do_abort(RPC_Cast, Nodes, Tid) ->
	for_each_node(Nodes,
		fun(Node) -> RPC_Cast(Node, {abort, Tid}) end,
		fun(_, _) -> ok end).

handle_cast(Request, State) ->
	{{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, Transactions, KeyRanges} = State,
	case Request of
		{keep_alive, From, To, Replication} ->
			NewKeyRanges = key_ranges:add(From, To, KeyRanges),
			send_keep_alive(RPC_Cast, Chord, From, To, Replication - 1),
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, Transactions, NewKeyRanges}}
	end.

handle_info(Request, State) ->
	{{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, Transactions, KeyRanges} = State,
	case Request of
		keep_alive ->
			Predecessor = chord:get_predecessor(Chord),
			send_keep_alive(RPC_Cast, Chord, Predecessor, crypto:sha(Name), Replication),
			{noreply, State};
		
		gc -> % Garbage collect unowned keys that no longer belong on this node.
			{Expired, NewKeyRanges} = key_ranges:expire(KeyRanges),
			if
				Expired -> spawn_link(fun() ->
					tm:for_each_key(TM, fun(Key) ->
						case key_ranges:in_ranges(Key, NewKeyRanges) of
							true -> keep_live_key;
							_ ->
								Predecessor = chord:get_predecessor(Chord),
								case key_ranges:in_half_open_range(Key, Predecessor, crypto:sha(Name)) of
									true -> keep_owned_key;
									_ -> tm:forget(TM, Key) % forget an unowned key that has expired
								end
						end
					end)
				end);
				true -> no_change
			end,
			{noreply, {{RPC_Call, RPC_Cast}, Gateway, Name, Chord, TM, Replication, Clock, Transactions, NewKeyRanges}}
	end.

send_keep_alive(_, _, _, _, Replication) when Replication =< 1 -> done;
send_keep_alive(RPC_Cast, Chord, From, To, Replication) ->
	Successor = chord:get_successor(Chord),
	RPC_Cast(Successor, {keep_alive, From, To, Replication}).

terminate(_Reason, _State) -> ok.

code_change(_Version, State, _Extra) -> {ok, State}.