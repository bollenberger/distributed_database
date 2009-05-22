% Transaction Manager
% Implements an MVCC transactional hash table.

% TODO - might want to parameterize storage to plug in ets, dets, mnesia or others.
% for now it's just ets
-module(tm).
-behaviour(gen_server).

-export([start_link/0, get/3, put/4, delete/3, prepare/2, commit/2, abort/2]).
-export([for_each_key/2, forget/2]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2, code_change/3]).

-define(VACUUM_INTERVAL, 20000).

start_link() ->
	gen_server:start_link(?MODULE, [], []).

init([]) ->
	{ok, _} = timer:send_after(?VACUUM_INTERVAL, vacuum),
	Table = ets:new(?MODULE, [ordered_set, protected]),
	{ok, {Table, dict:new(), 0, 0}}.

% returns {ok, Value, CreatedTid} or {no_value, DeletedTid} or throws {retry, Clock}
get(TM, Tid, Key) ->
	gen_server:call(TM, {get, Tid, Key}).

% returns ok or {retry, Clock}
put(TM, Tid, Key, Value) ->
	gen_server:call(TM, {put, Tid, Key, {ok, Value}}).

% returns ok or {retry, Clock}
delete(TM, Tid, Key) ->
	gen_server:call(TM, {put, Tid, Key, no_value}).

% returns ok or abort
prepare(TM, Tid) ->
	gen_server:call(TM, {prepare, Tid}).

% returns ok
commit(TM, Tid) ->
	gen_server:call(TM, {commit, Tid}).

% returns ok
abort(TM, Tid) ->
	gen_server:call(TM, {abort, Tid}).

% perform Operation for each key
for_each_key(TM, Operation) ->
	gen_server:call(TM, {for_each_key, Operation}).

% forget a range of keys (if it is owned by another node - for replication)
forget(TM, Key) ->
	gen_server:call(TM, {forget, Key}).

get_transaction(Transactions, MaxTid, Tid) ->
	case dict:find(Tid, Transactions) of
		{ok, BeginTransactions} -> {Transactions, BeginTransactions, MaxTid};
		error ->
			BeginTransactions = dict:fetch_keys(Transactions),
			NewTransactions = dict:store(Tid, BeginTransactions, Transactions),
			{NewTransactions, BeginTransactions, erlang:max(MaxTid, Tid)}
	end.

valid_reads(BeginTransactions, List) ->
	lists:filter(fun({Create, _Value, _LastRead, _Expired}) ->
		not(lists:member(Create, BeginTransactions))
	end, List).

conflicting_writes(BeginTransactions, List) ->
	lists:any(fun(Create) ->
		lists:member(Create, BeginTransactions)
	end, List).

translate_value(Data, Created) ->
	case Data of
		{ok, Value} -> {ok, Value, Created};
		no_value -> {no_value, Created}
	end.

handle_call(Request, Client, State) ->
	{Table, Transactions, Clock, MaxTid} = State,
	case Request of
		{get, Tid, Key} ->
			if
				Tid < Clock ->
					{reply, {retry, Clock}, {Table, dict:erase(Tid, Transactions), Clock, MaxTid}};
				true ->
					{NewTransactions, BeginTransactions, NewMaxTid} = get_transaction(Transactions, MaxTid, Tid),
					case ets:select(Table, [{
						{{Key, Tid}, '$1', uncommitted, '$2'},
						[],
						['$1']
					}]) of
						[] ->
							case valid_reads(BeginTransactions, ets:select(Table, [{
								{{Key, '$1'}, '$2', '$3', '$4'},
								[{'<', '$1', Tid}, {'=/=', '$3', uncommitted}, {'=/=', '$3', prepared}],
								[{{'$1', '$2', '$3', '$4'}}]
							}])) of
								[] ->
									{reply, {no_value, 0}, {Table, NewTransactions, Clock, NewMaxTid}};
								List ->
									{Create, Value, LastRead, Expired} = lists:last(List),
									if
										Tid > LastRead -> ets:insert(Table, {{Key, Create}, Value, Tid, Expired});
										true -> do_nothing
									end,
									{reply, translate_value(Value, Create), {Table, NewTransactions, Clock, NewMaxTid}}
							end;
						List ->
							{reply, translate_value(lists:last(List), Tid), {Table, NewTransactions, Clock, NewMaxTid}}
					end
			end;

		{put, Tid, Key, Value} ->
			{NewTransactions, BeginTransactions, NewMaxTid} = get_transaction(Transactions, MaxTid, Tid),
			
			% look for conflicting reads
			case ets:select(Table, [{
				{{Key, '$1'}, '$2', '$3', '$4'},
				[{'<', '$1', Tid}, {'<', Tid, '$3'}, {'=/=', uncommitted, '$3'}, {'=/=', prepared, '$3'}],
				['$3']
			}]) of
				[] -> % no conflict
					case conflicting_writes(BeginTransactions, ets:select(Table, [{
						{{Key, '$1'}, '$2', '$3', '$4'},
						[{'<', '$1', Tid}, {'=/=', '$3', uncommitted}, {'=/=', '$3', prepared}],
						['$1']
					}])) of
						true -> % conflicting write committed while this transaction was working.
							{reply, {retry, 0}, {Table, dict:erase(Tid, NewTransactions), Clock, NewMaxTid}};
						_ ->
							ets:insert(Table, {{Key, Tid}, Value, uncommitted, current}),
							{reply, ok, {Table, NewTransactions, Clock, NewMaxTid}}
					end;
				Clocks -> % conflict
					{reply, {retry, lists:max(Clocks)}, {Table, dict:erase(Tid, NewTransactions), Clock, NewMaxTid}}
			end;
		
		% On recovery, one would recover "prepared" entries, and discard "uncommitted" ones.
		{prepare, Tid} ->
			{NewTransactions, _, NewMaxTid} = get_transaction(Transactions, MaxTid, Tid),
			lists:map(fun([Key, Value, Expired]) ->
				ets:insert(Table, {{Key, Tid}, Value, prepared, Expired})
			end, ets:match(Table, {{'$1', Tid}, '$2', uncommitted, '$3'})),
			{reply, ok, {Table, NewTransactions, Clock, NewMaxTid}};
		
		{commit, Tid} ->
			{NewTransactions, _, NewMaxTid} = get_transaction(Transactions, MaxTid, Tid),
			
			lists:map(fun([Key, Value, Expired]) ->
				% expire existing entries
				Existing = ets:match(Table, {{Key, '$1'}, '$2', '$3', current}),
				lists:map(fun([Create, OldValue, Read]) ->
					ets:insert(Table, {{Key, Create}, OldValue, Read, Tid})
				end, Existing),

				% commit this entry
				ets:insert(Table, {{Key, Tid}, Value, Tid, Expired})
			end, ets:match(Table, {{'$1', Tid}, '$2', prepared, '$3'})),
			{reply, ok, {Table, dict:erase(Tid, NewTransactions), Clock, NewMaxTid}};
		
		{abort, Tid} ->
			{NewTransactions, _, NewMaxTid} = get_transaction(Transactions, MaxTid, Tid),
			ets:match_delete(Table, {{'$1', Tid}, '$2', '$3', '$4'}),
			{reply, ok, {Table, dict:erase(Tid, NewTransactions), Clock, NewMaxTid}};
		
		{for_each_key, Operation} ->
			spawn_link(fun() ->
				case ets:select(Table, [{
					{{'$1', '$2'}, '$3', '$4', current},
					['$1']
				}], 100) of
					{Match, Continuation} -> do_for_each_key(Match, Continuation, Operation);
					'$end_of_table' -> done
				end,
				gen_server:reply(Client, ok)
			end),
			{noreply, State};
		
		{forget, Key} -> % Completely forget a given key. This is for replication garbage collection. For ordinarily deleting an element, use delete.
			ets:select_delete(Table, [{
				{{Key, '$1'}, '$2', '$3', '$4'},
				[],
				[true]
			}]),
			{reply, ok, State}
	end.

do_for_each_key([], Continuation, Operation) ->
	case ets:select(Continuation) of
		'$end_of_table' -> done;
		{Match, NewContinuation} -> do_for_each_key(Match, NewContinuation, Operation)
	end;
do_for_each_key([Key|Rest], Continuation, Operation) ->
	Operation(Key),
	do_for_each_key(Rest, Continuation, Operation).
	

vacuum(State) ->
	{Table, Transactions, _Clock, MaxTid} = State,
	NewClock = lists:min([MaxTid|dict:fetch_keys(Transactions)]),
	ets:select_delete(Table, [{
		{{'$1', '$2'}, '$3', '$4', '$5'},
		[{'<', '$2', NewClock}, {'=/=', '$5', current}],
		[true]
	}]),
	{ok, _} = timer:send_after(?VACUUM_INTERVAL, vacuum),
	{Table, Transactions, NewClock, MaxTid}.

handle_cast(_Request, State) ->
	{noreply, State}.

handle_info(Request, State) ->
	case Request of
		vacuum -> {noreply, vacuum(State)}
	end.

terminate(_Reason, {Table, _, _, _}) ->
	ets:delete(Table),	
	ok.

code_change(_Version, State, _Extra) -> {ok, State}.