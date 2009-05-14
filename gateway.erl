% The abstraction provided here is one of a service-based message router.
% A node starts its gateway with start_link(), and then listen()s on zero or
% more local address/port pairs, and/or connects to zero or more remote address/port
% pairs. The gateway will then remain responsible for maintaining its outgoing
% connections. If the connection is lost, it will be reestablished automatically when and if
% remote listener returns.
%
% A service (any client of the gateway) operates by registering itself with the gateway
% under a unique name. A service may consist of service instances running on one or
% more nodes. The service name is added to the routing tables and propagated according to the
% same routing algortihm as individual node names. Each service registers a handler, which can
% handle "call" and/or "cast" requests from other nodes. Call requests are synchronous and expect
% a response. Cast requests are asynchronous messages, requiring no direct response to the caller.
% The registration of a service is performed with the register() function. The handler is a function
% that takes three parameters: call or cast, the name of the sending node, and the message. In the case
% of a call, the return value of the handler is returned to the caller as {ok, Value}.
%
% Messages may be lost in transit, so messages are unreliable in general. The
% expected behavior is to send and forget or to timeout and fail if waiting
% for a message for longer than expected.
%
% The routing algorithm is a link vector algorithm. When nodes join or leave,
% local routing tables are updated at the peers and the changes are propagated out
% immediately, but asynchronously.
%
% Author: briano

-module(gateway).
-behaviour(gen_server).

-export([test/0]).

% API
-export([start_link/0, start_link/1, listen/3, connect/4]).
-export([register/3, get_name/1]).
-export([call/1, call/2, call/3, call/4, call/5, call_group/1, call_group/2, call_group/3, call_group/4]).
-export([cast/1, cast/2, cast/3, cast/4, cast_group/1, cast_group/2, cast_group/3]).

-export([init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2, code_change/3]).

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
	run_test(fun test_register/0).

test_register() ->
	{ok, A} = start_link(),
	listen(A, {127,0,0,1}, 1234),
	
	{ok, B} = start_link(),
	listen(B, {127,0,0,1}, 1235),
	
	{ok, C} = start_link(),
	listen(C, {127,0,0,1}, 1236),
	
	Cname = get_name(C),
	connect(A, {127,0,0,1}, 1235, 1), % A->B
	connect(B, {127,0,0,1}, 1236, 1), % B->C
	
	TestPid = self(),
	ok = register(A, double, fun
		(call, _, X) -> X * 2;
		(cast, From, X) -> TestPid ! {From, X}
	end),

	Ccaller = call_group(C, 5000, double),
	Ccaster = cast_group(C, double),
	
	receive after 100 -> ok end, % wait for routing to get set up

	{ok, 8} = Ccaller(4),
	Ccaster(foo),
	receive {From, foo} ->
		if Cname =:= From -> ok;
		   true -> throw(mismatch)
		end
	end.

% API

start_link() ->
	case crypto:start() of
		ok -> ok;
		{error, {already_started, crypto}} -> ok
	end,
	MyID = guid:new(),
	start_link(MyID).
start_link(MyID) ->
	gen_server:start_link(?MODULE, [MyID], []).

listen(Server, Address, Port) ->
	gen_server:cast(Server, {listen, Address, Port}).

connect(Server, Address, Port, Metric) ->
	gen_server:cast(Server, {connect, Address, Port, Metric}).

send_message(Server, To, ServiceName, Message) ->
	gen_server:call(Server, {send_message, To, ServiceName, Message}).

get_name(Server) ->
	gen_server:call(Server, {get_name}).

% register adds this node to a group named Group. Any node may send a message
% to that group and the message will go to the nearest node in that group.
% Pure clients may call a service using call() or cast()
register(Server, ServiceName, Handler) ->
	gen_server:call(Server, {register, ServiceName, Handler}).

% Provide a bunch of constructors to curry call() and cast() as much or as little as you like.
% The functions that take all parameters perform the operation rather than returning a function that performs the operation.
call(Server) ->
	fun(Timeout, ServiceName, ToID, Params) ->
		call(Server, Timeout, ServiceName, ToID, Params)
	end.
call(Server, Timeout) ->
	fun(ServiceName, ToID, Params) ->
		call(Server, Timeout, ServiceName, ToID, Params)
	end.
call(Server, Timeout, ServiceName) ->
	fun(ToID, Params) ->
		call(Server, Timeout, ServiceName, ToID, Params)
	end.
call(Server, Timeout, ServiceName, ToID) ->
	fun(Params) ->
		call(Server, Timeout, ServiceName, ToID, Params)
	end.
call(Server, Timeout, ServiceName, ToID, Params) ->
	do_call_handle(gen_server:call(Server, {call, Timeout, ServiceName, ToID, Params}, rpc_timeout(Timeout))).
rpc_timeout(N) -> N + 5000.

call_group(Server) ->
	fun(Timeout, ServiceName, Params) ->
		call_group(Server, Timeout, ServiceName, Params)
	end.
call_group(Server, Timeout) ->
	fun(ServiceName, Params) ->
		call_group(Server, Timeout, ServiceName, Params)
	end.
call_group(Server, Timeout, ServiceName) ->
	fun(Params) ->
		call_group(Server, Timeout, ServiceName, Params)
	end.
call_group(Server, Timeout, ServiceName, Params) ->
	call(Server, Timeout, ServiceName, ServiceName, Params).

cast(Server) ->
	fun(ServiceName, ToID, Params) ->
		cast(Server, ServiceName, ToID, Params)
	end.
cast(Server, ServiceName) ->
	fun(ToID, Params) ->
		cast(Server, ServiceName, ToID, Params)
	end.
cast(Server, ServiceName, ToID) ->
	fun(Params) ->
		cast(Server, ServiceName, ToID, Params)
	end.
cast(Server, ServiceName, ToID, Params) ->
	gen_server:cast(Server, {cast, ServiceName, ToID, Params}).

cast_group(Server) ->
	fun(ServiceName, Params) ->
		cast_group(Server, ServiceName, Params)
	end.
cast_group(Server, ServiceName) ->
	fun(Params) ->
		cast_group(Server, ServiceName, Params)
	end.
cast_group(Server, ServiceName, Params) ->
	cast(Server, ServiceName, ServiceName, Params).


% gen_server

init([MyID]) ->
	{ok, HeartbeatTimer} = timer:send_interval(15000, heartbeat),
	{ok, {dict:new(), MyID, dict:new(), dict:new(), dict:new(), dict:new(), {HeartbeatTimer}}}.

listener(Server, MyID, ListenSocket) ->
	link(Server),
	case gen_tcp:accept(ListenSocket) of
		{ok, Socket} ->
			try
				spawn(fun() -> listener(Server, MyID, ListenSocket) end),
				{ok, Metric} = receive_metric(Socket),
				{ok, PeerID} = negotiate_id(Socket, MyID),
				ok = add_peer(Server, PeerID, self(), Metric),
				worker(Server, Socket, MyID, PeerID)
			after
				gen_tcp:close(Socket)
			end;
		{error, closed} -> shutdown
	end.

% Middle Man
worker(Server, Socket, MyID, PeerID) ->
	inet:setopts(Socket, [{active, once}]),
	receive
		{send_message, FromID, ToID, Message} ->
			gen_tcp:send(Socket, term_to_binary({FromID, ToID, Message})),
			worker(Server, Socket, MyID, PeerID);
		{tcp, Socket, Data} ->
			{FromID, ToID, Message} = binary_to_term(Data),
			case ToID of
				MyID ->
					ok = receive_message(Server, FromID, Message),
					worker(Server, Socket, MyID, PeerID);
				_ ->
					ok = forward_message(Server, FromID, ToID, Message),
					worker(Server, Socket, MyID, PeerID)
			end;
		{tcp_closed, Socket} ->
			connection_closed
	end.

% A synthetic middle man for receiving messages to a group service that this node
% is a member of.
group_worker(Server, ServiceName) ->
	receive
		{send_message, FromID, _GroupServiceName, {message, ServiceName, Message}} ->
			receive_message(Server, FromID, {message, ServiceName, Message}),
			group_worker(Server, ServiceName);
		_ -> group_worker(Server, ServiceName)
	end.

receive_message(Server, FromID, Message) ->
	gen_server:cast(Server, {receive_message, FromID, Message}).

forward_message(Server, FromID, ToID, Message) ->
	gen_server:call(Server, {forward_message, FromID, ToID, Message}).

add_peer(Server, PeerID, Peer, Metric) ->
	gen_server:call(Server, {add_peer, PeerID, Peer, Metric}).

remove_peer(Server, Peer) ->
	gen_server:call(Server, {remove_peer, Peer}).

do_add_peer({Services, MyID, Peers, PeerIDs, Metrics, Routes, Timers}, PeerID, Peer, Metric) ->
	NewPeers = dict:store(PeerID, Peer, Peers),
	NewPeerIDs = dict:store(Peer, PeerID, PeerIDs),
	NewMetrics = dict:store(PeerID, Metric, Metrics),
	NewRoutes = routes:add(PeerID, Peer, Metric, Routes, notify_peers(NewPeers, Peer, MyID)),
	routes:best(Routes, fun(Dest, MetricToDest) ->
		Peer ! {send_message, MyID, PeerID, {link, Dest, MetricToDest}}
	end),
	{Services, MyID, NewPeers, NewPeerIDs, NewMetrics, NewRoutes, Timers}.

receive_metric(Socket) ->
	receive
		{tcp, Socket, Data} ->
			{metric, Metric} = binary_to_term(Data),
			{ok, Metric}
	end.
send_metric(Socket, Metric) ->
	gen_tcp:send(Socket, term_to_binary({metric, Metric})).

negotiate_id(Socket, MyID) -> negotiate_id(Socket, MyID, infinity).
negotiate_id(Socket, MyID, Timeout) ->
	gen_tcp:send(Socket, term_to_binary({id, MyID})),
	inet:setopts(Socket, [{active, once}]),
	receive
		{tcp, Socket, Data} ->
			{id, RemoteID} = binary_to_term(Data),
			{ok, RemoteID}
	after
		Timeout -> negotiate_id_timeout
	end.

do_send_message(Server, MyID, MyID, Message, _) -> % loopback message
	receive_message(Server, MyID, Message);
do_send_message(_, FromID, ToID, Message, Routes) ->
	case routes:route_to(ToID, Routes) of
		{ok, Router} ->	Router ! {send_message, FromID, ToID, Message};
		no_route -> drop_message
	end.

% notify_peers/4 actually notifies the peers of whatever message they are given
notify_peers(Peers, Except, Message, MyID) when not(is_list(Peers)) ->
	notify_peers(dict:to_list(Peers), Except, Message, MyID);
notify_peers([], _, _, _) -> ok;
notify_peers([{_, Except}|Rest], Except, Message, MyID) ->
	notify_peers(Rest, Except, Message, MyID);
notify_peers([{PeerID, Peer}|Rest], Except, Message, MyID) ->
	%io:format("~p tells ~p that it can route to ~p with metric ~p~n", [MyID, PeerID, Dest, Metric]),
	Peer ! {send_message, MyID, PeerID, Message},
	notify_peers(Rest, Except, Message, MyID).

% notify_peers/3 returns a function that notifies all peers in Peers, except for Except of a new metric to a destination
notify_peers(Peers, Except, MyID) ->
	fun(Dest, Metric) ->
		spawn(fun() ->
			notify_peers(Peers, Except, {link, Dest, Metric}, MyID)
		end)
	end.

do_cast(Server, ToID, ServiceName, Cast) ->
	send_message(Server, ToID, ServiceName, {cast, Cast}), ok.

do_call_handle(Result) ->
	case Result of
		{throw, Error} -> throw(Error);
		{error, Error} -> erlang:error(Error);
		{exit, Error} -> exit(Error);
		{ok, X} -> {ok, X};
		timeout -> timeout
	end.

do_call(Server, Timeout, ToID, ServiceName, Call) ->
	CallID = guid:new(),
	send_message(Server, ToID, ServiceName, {call, self(), CallID, Call}),
	receive
		{response, ServiceName, CallID, _ReplyFromID, Response} -> Response
	after Timeout -> timeout
	end.

next_wait(0) -> 50; % start at 50 ms
next_wait(Wait) when Wait >= 10000 -> 10000; % max 10 s wait
next_wait(Wait) -> Wait * 2. % double the wait each time (exponential)

do_connect(Server, MyID, Address, Port, Metric, Wait) ->
	receive after Wait -> ok end,
	case gen_tcp:connect(Address, Port, [binary, {active, once}, {packet, 4}]) of
		{ok, Socket} ->
			send_metric(Socket, Metric),
			{ok, PeerID} = negotiate_id(Socket, MyID),
			add_peer(Server, PeerID, self(), Metric),
			try
				worker(Server, Socket, MyID, PeerID)
			after
				gen_tcp:close(Socket),
				remove_peer(Server, self()),
				do_connect(Server, MyID, Address, Port, Metric, 0) % automatically reconnect
			end;
		{error, _} -> % retry after a delay
			do_connect(Server, MyID, Address, Port, Metric, next_wait(Wait))
	end.

handle_call(Request, Client, State) ->
	{Services, MyID, Peers, PeerIDs, Metrics, Routes, Timers} = State,
	case Request of
		{add_peer, PeerID, Peer, Metric} ->
			{reply, ok, do_add_peer(State, PeerID, Peer, Metric)};
		
		{remove_peer, Peer} ->
			case dict:find(Peer, PeerIDs) of
				{ok, PeerID} ->
					NewPeers = dict:erase(PeerID, Peers),
					NewPeerIDs = dict:erase(Peer, PeerIDs),
					NewMetrics = dict:erase(PeerID, Metrics),
					NewRoutes = routes:remove_through(Peer, Routes, notify_peers(NewPeers, nil, MyID)),
					{reply, ok, {Services, MyID, NewPeers, NewPeerIDs, NewMetrics, NewRoutes, Timers}};
				error -> {reply, ok, State}
			end;

		{send_message, ToID, ServiceName, Message} ->
			Server = self(),
			spawn_link(fun() ->
				try
					do_send_message(Server, MyID, ToID, {message, ServiceName, Message}, Routes)
				after
					gen_server:reply(Client, ok)
				end
			end),
			{noreply, State};
		{forward_message, FromID, ToID, Message} ->
			do_send_message(self(), FromID, ToID, Message, Routes),
			{reply, ok, State};

		{register, ServiceName, Handler} ->
			case dict:find(ServiceName, Services) of
				{ok, _} ->
					{reply, service_exists, State};
				error ->
  					Server = self(),
					NewServices = dict:store(ServiceName, spawn_link(fun() ->
						rpc_loop(Server, ServiceName, Handler)
					end), Services),
					Peer = spawn_link(fun() ->
						group_worker(Server, ServiceName)
					end),
					NewState = do_add_peer({NewServices, MyID, Peers, PeerIDs, Metrics, Routes, Timers}, ServiceName, Peer, 0),
					
					{reply, ok, NewState}
			end;

		{call, Timeout, ServiceName, ToID, Call} ->
			Server = self(),
			spawn_link(fun() ->
				gen_server:reply(Client, do_call(Server, Timeout, ToID, ServiceName, Call))
			end),
			{noreply, State};
		
		{get_name} ->
			{reply, MyID, State}
	end.

handle_cast(Request, State) ->
	{Services, MyID, Peers, PeerIDs, Metrics, Routes, Timers} = State,
	case Request of
		{listen, Address, Port} ->
			{ok, ListenSocket} = gen_tcp:listen(Port, [binary, {ip, Address}, {active, once}, {reuseaddr, true}, {packet, 4}]),
			Server = self(),
			spawn_link(fun() -> listener(Server, MyID, ListenSocket) end),
			{noreply, State};
		
		{connect, Address, Port, Metric} ->
			Server = self(),
			spawn_link(fun() -> do_connect(Server, MyID, Address, Port, Metric, 0) end),
			{noreply, State};
		
		{receive_message, _FromID, heartbeat} ->
			{noreply, State};
		{receive_message, FromID, {link, Dest, Metric}} ->
			%io:format("~p learned that ~p can route to ~p with metric ~p~n", [MyID, FromID, Dest, Metric]),
			Peer = dict:fetch(FromID, Peers),
			HopMetric = dict:fetch(FromID, Metrics),
			RouteMetric = Metric + HopMetric,
			NewRoutes = routes:add(Dest, Peer, RouteMetric, Routes, notify_peers(Peers, Peer, MyID)),
			{noreply, {Services, MyID, Peers, PeerIDs, Metrics, NewRoutes, Timers}};
		
		{receive_message, FromID, {message, ServiceName, Message}} ->
			%io:format("receive ~p ~p ~p->~p: ~p~n~p~n", [MyID, ServiceName, FromID, MyID, Message, Services]),
			case Message of
				{response, RespondTo, CallID, Response} ->
					RespondTo ! {response, ServiceName, CallID, FromID, Response};
				_ -> % call or cast
					case dict:find(ServiceName, Services) of
						{ok, Service} ->
							Service ! {gateway, self(), FromID, ServiceName, Message};
						error ->
							io:format("DROPPED A MESSAGE~n"),
							drop_message
					end
			end,
			{noreply, State};

		{cast, ServiceName, ToID, Cast} ->
			Server = self(),
			spawn_link(fun() ->
				do_cast(Server, ToID, ServiceName, Cast)
			end),
			{noreply, State}
	end.

rpc_loop(Gateway, ServiceName, Handler) ->
	receive
		{gateway, Gateway, FromID, ServiceName, {cast, Cast}} ->
			spawn_link(fun() ->
				try
					Handler(cast, FromID, Cast)
				catch
					_:Error -> Error
				end
			end);

		{gateway, Gateway, FromID, ServiceName, {call, RespondTo, CallID, Call}} -> % remote call incoming
			spawn_link(fun() ->
				Response = try
					{ok, Handler(call, FromID, Call)}
				catch
					throw:Error -> {throw, Error};
					error:Error -> {error, Error};
					exit:Error -> {exit, Error}
				end,
				send_message(Gateway, FromID, ServiceName, {response, RespondTo, CallID, Response})
			end);
		
		Other -> io:format("unexpected: ~p", [Other])
	end,
	rpc_loop(Gateway, ServiceName, Handler).

handle_info(Info, State) ->
	{_Services, MyID, Peers, _PeerIDs, _Metrics, _Routes, _Timers} = State,
	case Info of
		heartbeat ->
			notify_peers(Peers, except_none, heartbeat, MyID),
			{noreply, State}
	end.

terminate(_Reason, State) ->
	{_Services, _MyID, _Peers, _PeerIDs, _Metrics, _Routes, Timers} = State,
	{HeartbeatTimer} = Timers,
	{ok, cancel} = timer:cancel(HeartbeatTimer),
	ok.

code_change(_Version, State, _Extra) -> {ok, State}.