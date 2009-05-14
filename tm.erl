% Transaction Manager
% Implements an MVCC transactional hash table.

% TODO - might want to parameterize storage to plug in ets, dets, mnesia or others.
% for now it's just ets
-module(tm).
-behaviour(gen_server).

-export([start_link/0, get/3, put/4, delete/3, prepare/2, commit/2, abort/2]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2, code_change/3]).

start_link() ->
	gen_server:start_link(?MODULE, [], []).

init([]) ->
	{ok, VacuumTimer} = timer:send_interval(2000, vacuum),
	Timers = {VacuumTimer},
	Table = ets:new(?MODULE, [ordered_set, private]),
	{ok, {Table, dict:new(), 0, 0, Timers}}.

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

handle_call(Request, _Client, State) ->
	{Table, Transactions, Clock, MaxTid, Timers} = State,
	case Request of
		{get, Tid, Key} ->
			if
				Tid < Clock ->
					{reply, {retry, Clock}, {Table, dict:erase(Tid, Transactions), Clock, MaxTid, Timers}};
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
									{reply, {no_value, 0}, {Table, NewTransactions, Clock, NewMaxTid, Timers}};
								List ->
									{Create, Value, LastRead, Expired} = lists:last(List),
									if
										Tid > LastRead -> ets:insert(Table, {{Key, Create}, Value, Tid, Expired});
										true -> do_nothing
									end,
									{reply, translate_value(Value, Create), {Table, NewTransactions, Clock, NewMaxTid, Timers}}
							end;
						List ->
							{reply, translate_value(lists:last(List), Tid), {Table, NewTransactions, Clock, NewMaxTid, Timers}}
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
						true -> % conflicting write committed while we this transaction was working.
							{reply, {retry, 0}, {Table, dict:erase(Tid, NewTransactions), Clock, NewMaxTid, Timers}};
						_ ->
							ets:insert(Table, {{Key, Tid}, Value, uncommitted, current}),
							{reply, ok, {Table, NewTransactions, Clock, NewMaxTid, Timers}}
					end;
				Clocks -> % conflict
					{reply, {retry, lists:max(Clocks)}, {Table, dict:erase(Tid, NewTransactions), Clock, NewMaxTid, Timers}}
			end;
		
		% On recovery, one would recover "prepared" entires, and discard "uncommitted" ones.
		{prepare, Tid} ->
			{NewTransactions, _, NewMaxTid} = get_transaction(Transactions, MaxTid, Tid),
			lists:map(fun([Key, Value, Expired]) ->
				ets:insert(Table, {{Key, Tid}, Value, prepared, Expired})
			end, ets:match(Table, {{'$1', Tid}, '$2', uncommitted, '$3'})),
			{reply, ok, {Table, NewTransactions, Clock, NewMaxTid, Timers}};
		
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
			{reply, ok, {Table, dict:erase(Tid, NewTransactions), Clock, NewMaxTid, Timers}};
		
		{abort, Tid} ->
			{NewTransactions, _, NewMaxTid} = get_transaction(Transactions, MaxTid, Tid),
			ets:match_delete(Table, {{'$1', Tid}, '$2', '$3', '$4'}),
			{reply, ok, {Table, dict:erase(Tid, NewTransactions), Clock, NewMaxTid, Timers}}
	end.

vacuum(State) ->
	{Table, Transactions, _Clock, MaxTid, Timers} = State,
	NewClock = lists:min([MaxTid|dict:fetch_keys(Transactions)]),
	ets:select_delete(Table, [{
		{{'$1', '$2'}, '$3', '$4', '$5'},
		[{'<', '$2', NewClock}, {'=/=', '$5', current}],
		[true]
	}]),
	{Table, Transactions, NewClock, MaxTid, Timers}.

handle_cast(_Request, State) ->
	{noreply, State}.

handle_info(Request, State) ->
	case Request of
		vacuum -> {noreply, vacuum(State)}
	end.

terminate(_Reason, {Table, _, _, _, Timers}) ->
	{VacuumTimer} = Timers,
	{ok, cancel} = timer:cancel(VacuumTimer),
	
	ets:delete(Table),
	
	ok.

code_change(_Version, State, _Extra) -> {ok, State}.