-module(guid).

-export([new/0]).

% Generates a GUID per http://www.ietf.org/rfc/rfc4122.txt
% Requires crypto:start() to have been called.
new() ->
	EpochTime = calendar:datetime_to_gregorian_seconds({{1582,
		10, 15}, {0, 0, 0}}) * 10000000,
	{_MegaSecs, _Secs, MicroSecs} = now(),
	Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()) * 10000000 + MicroSecs * 10,
	Time = Now - EpochTime,
	<<TimeHigh:2/binary, TimeMid:2/binary, TimeLow:4/binary>> = <<1:4, Time:60/big>>,
	Clock = crypto:rand_bytes(2),
	NodeBytes = crypto:rand_bytes(5),
	NodeMSB = crypto:rand_uniform(0, 256) bor 1,
	Node = <<NodeMSB, NodeBytes/binary>>,
	<<TimeLow/binary, TimeMid/binary, TimeHigh/binary, Clock/binary, Node/binary>>.