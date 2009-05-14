-module(stream).
-behaviour(gen_server).

-export([test/0]).
-export([start_link/1, make_stream/1, make_sender/2]).
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
	run_test(fun test_channel_send/0).

test_channel_send() ->
	{ok, Gateway1} = gateway:start_link(),
	gateway:listen(Gateway1, {127,0,0,1}, 1234),
	{ok, Gateway2} = gateway:start_link(),
	gateway:connect(Gateway2, {127,0,0,1}, 1234, 1),
	
	{ok, Stream1} = stream:start_link(Gateway1),
	{ok, Stream2} = stream:start_link(Gateway2),
	
	{Local, Remote} = stream:make_stream(Stream1),
	
	Sender = stream:make_sender(Stream2, Remote),
	
	Sender ! <<"this is a test">>,
	Sender ! close,
	
	receive
		{packet, Local, Data} -> io:format("~p~n", [Data])
	end.

start_link(Gateway) ->
	gen_server:start_link(?MODULE, [Gateway], []).
	
init([Gateway]) ->
	Name = gateway:get_name(Gateway),
	Handler = fun
		(cast, _, {heartbeat, Stream, Pid}) ->
			Pid ! {heartbeat, Stream};
		(cast, From, {packet, Stream, Pid, Sequence, Data, AckPid}) ->
			Pid ! {packet, From, Stream, Sequence, Data, AckPid}; % to stream_worker()
		(cast, _, {ack, Stream, Pid, Sequence}) ->
			Pid ! {ack, Stream, Sequence}; % to send()
		(cast, _, {close, Stream, Pid, Sequence}) ->
			Pid ! {close, Stream, Sequence} % to stream_worker()
	end,
	
	ok = gateway:register(Gateway, ?MODULE, Handler),
	RPC_Cast = gateway:cast(Gateway, ?MODULE),
	
	{ok, {RPC_Cast, Name}}.
	
stream_worker(RPC_Cast, Stream, Pid, Sequence) ->
	InactivityTimeout = 300000, % 30 seconds - three times the 10 second heartbeat
	receive
		{heartbeat, Stream} ->
			% reset inactivity timeout
			stream_worker(RPC_Cast, Stream, Pid, Sequence);
		{packet, From, Stream, Sequence, Data, AckPid} ->
			Pid ! {packet, Stream, Data},
			RPC_Cast(From, {ack, Stream, AckPid, Sequence}),
			stream_worker(RPC_Cast, Stream, Pid, Sequence+1);
		{packet, _, Stream, S, _, _} when S < Sequence ->
			drop_duplicate,
			stream_worker(RPC_Cast, Stream, Pid, Sequence);
		{close, Stream, Sequence} ->
					Pid ! {close, Stream},
			terminate
	after InactivityTimeout ->
		terminate % just give up. the remote end will give up eventually
	end.

sender_loop(Server, Channel) ->
	receive
		close ->
			close(Server, Channel);
		Data when is_binary(Data) ->
			case send(Server, Channel, Data) of
				{ok, NewChannel} -> sender_loop(Server, NewChannel);
				timeout -> terminate
			end;
		{'EXIT', _, _} ->
			close(Server, Channel)
	after 10000 ->
		heartbeat(Server, Channel),
		sender_loop(Server, Channel)
	end.

make_sender(Server, Channel) ->
	spawn_link(fun() ->
		process_flag(trap_exit, true),
		sender_loop(Server, Channel)
	end).

% Send a periodic heartbeat message to keep the session alive.
heartbeat(Server, Channel) ->
	gen_server:cast(Server, {heartbeat, Channel}).

% Close a channel's write end.
close(Server, Channel) ->
	gen_server:call(Server, {close, Channel}).

% Synchronously send data. Try to send bigger chunks at once. The bigger the better. They will be chunked if need be.
% returns {ok, NewChannel} or timeout
send(Server, Channel, Data) ->
	gen_server:call(Server, {send, Channel, Data}, 30000).

send_chunk(_, _, _, _, _, _, 0) -> timeout;
send_chunk(RPC_Cast, Name, Stream, Pid, Sequence, Chunk, Retry) ->
	RPC_Cast(Name, {packet, Stream, Pid, Sequence, Chunk, self()}),
	receive
		{ack, Stream, Sequence} -> ok
	after 1000 ->
		send_chunk(RPC_Cast, Name, Stream, Pid, Sequence, Chunk, Retry-1)
	end.

% returns {ok, NewChannel} or timeout
send_chunks(_RPC_Cast, Name, Stream, Pid, Sequence, <<>>) -> {ok, {Name, Stream, Pid, Sequence}};
send_chunks(RPC_Cast, Name, Stream, Pid, Sequence, <<Chunk:4096/binary, Rest/binary>>) ->
	send_chunks(RPC_Cast, Name, Stream, Pid, Sequence, Chunk, Rest);
send_chunks(RPC_Cast, Name, Stream, Pid, Sequence, Chunk) ->
	send_chunks(RPC_Cast, Name, Stream,Pid, Sequence, Chunk, <<>>).
send_chunks(RPC_Cast, Name, Stream, Pid, Sequence, Chunk, Rest) ->
	case send_chunk(RPC_Cast, Name, Stream, Pid, Sequence, Chunk, 5) of
			ok ->
				send_chunks(RPC_Cast, Name, Stream, Pid, Sequence+1, Rest);
			timeout ->
				timeout
	end.

% Returns Channel.
make_stream(Server) ->
	gen_server:call(Server, {make_stream, self()}).

handle_call(Request, _Client, State) ->
	{RPC_Cast, MyName} = State,
	case Request of
		{make_stream, Pid} ->
			Stream = guid:new(),
			InitSequence = 0,
			StreamPid = spawn_link(fun() -> stream_worker(RPC_Cast, Stream, Pid, InitSequence) end),
			{reply, {Stream, {MyName, Stream, StreamPid, InitSequence}}, State};
			
		{send, Channel, Data} ->
			{Name, Stream, Pid, Sequence} = Channel,
			Result = send_chunks(RPC_Cast, Name, Stream, Pid, Sequence, Data),
			{reply, Result, State};
		
		{close, Channel} ->
			{Name, Stream, Pid, Sequence} = Channel,
			RPC_Cast(Name, {close, Stream, Pid, Sequence}),
			{reply, ok, State}
	end.
	
handle_cast(Request, State) ->
	{RPC_Cast, _MyName} = State,
	case Request of
		{heartbeat, Channel} ->
			{Name, Stream, Pid, _Sequence} = Channel,
			RPC_Cast(Name, {heartbeat, Stream, Pid}),
			{noreply, State}
	end.

handle_info(_Request, State) -> {noreply, State}.
	
terminate(_Reason, _State) ->
	ok.

code_change(_Version, State, _Extra) -> {ok, State}.