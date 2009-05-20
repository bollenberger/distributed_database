-module(deploy).
-behaviour(gen_server).

-export([start_link/3, update/3, get_fingers/1, version/0]).
-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2, code_change/3]).

get_fingers(Deploy) ->
	gen_server:call(Deploy, get_fingers).

version() -> 600.

start_link(Gateway, ServiceName, Bootstrap) ->
	gen_server:start_link(?MODULE, [Gateway, {?MODULE, ServiceName}, Bootstrap], []).
	
init([Gateway, ServiceName, Bootstrap]) ->
	{ok, Chord} = chord:start_link(Gateway, ServiceName, Bootstrap),
	{ok, Stream} = stream:start_link(Gateway, ServiceName),
	Deploy = self(),
	
	Cast = gateway:cast(Gateway, ServiceName),
	Handler = fun
		(cast, From, {notify, Modules, Version}) ->
			notify(Deploy, From, Modules, Version);
			
		(cast, _, {get_module, Module, Channel}) ->
			Sender = stream:make_sender(Stream, Channel),
			{Module, Binary, _} = code:get_object_code(Module),
			Sender(Binary),
			Sender(close)
	end,
	gateway:register(Gateway, ServiceName, Handler),
	
	{ok, {Cast, Chord, Stream, 0}}.
	
module_reader(Module, File, Channel) -> module_reader([], Module, File, Channel).
module_reader(Acc, Module, File, Channel) ->
	stream:ready_to_receive(Channel),
	receive
		{packet, Channel, Data} ->
			module_reader([Data|Acc], Module, File, Channel);
		{close, Channel} ->
			io:format("loading ~p~n", [Module]),
			{module, Module} = code:load_binary(Module, File, iolist_to_binary(lists:reverse(Acc)))
	after
		30000 -> % inactivity timeout
			give_up
	end.
	
get_modules(_, _, _, []) -> done;
get_modules(Cast, Stream, From, [{Module, File}|Rest]) ->
	io:format("get_modules ~p~n", [Module]),
	Channel = stream:make_channel(Stream),
	Cast(From, {get_module, Module, Channel}),
	module_reader(Module, File, Channel),
	get_modules(Cast, Stream, From, Rest).

% Notify peers of a new version
update(Deploy, Modules, Version) ->
	gen_server:cast(Deploy, {update, Modules, Version}).

notify(Deploy, From, Modules, Version) ->
	gen_server:cast(Deploy, {notify, From, Modules, Version}).

handle_call(Request, _Client, State) ->
	{_, Chord, _, _} = State,
	case Request of
		get_fingers ->
			{reply, chord:get_fingers(Chord), State}
	end.

handle_cast(Request, State) ->
	{Cast, Chord, Stream, OldVersion} = State,
	case Request of
		{update, Modules, NewVersion} ->
			spawn_link(fun() ->
				% For each Chord finger
				lists:map(fun(Node) ->
					io:format("node: ~p~n", [Node]),
					Mods = lists:map(fun(Module) ->
						{Module, code:which(Module)}
					end, Modules),
					Cast(Node, {notify, Mods, NewVersion})
				end, chord:get_fingers(Chord))
			end),
			{noreply, {Cast, Chord, Stream, NewVersion}};
		
		{notify, From, Modules, NewVersion} when NewVersion > OldVersion ->
			Deploy = self(),
			spawn_link(fun() ->
				io:format("notified ~p ~p~n", [Modules, NewVersion]),
				get_modules(Cast, Stream, From, Modules),
				update(Deploy, lists:map(fun({Module, _}) -> Module end, Modules), NewVersion)
			end),
			{noreply, {Cast, Chord, Stream, NewVersion}};
		{notify, _, _, _} ->
			{noreply, State}
	end.

handle_info(_Request, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_Version, State, _Extra) -> {ok, State}.