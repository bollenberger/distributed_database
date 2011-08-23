% A data structure that manages a list of key ranges (from, to]

-module(key_ranges).

-export([add/3, in_ranges/2, expire/1]).
-export([in_open_range/3, in_half_open_range/3]).

% Add a range to the range list.
add(From, To, Ranges) -> add([], From, To, Ranges).
add(Acc, From, To, []) -> [{From, To, 1}|Acc];
add(Acc, From, To, [{From, To, _}|Rest]) -> Acc ++ [{From, To, 1}|Rest];
add(Acc, From, To, [Entry|Rest]) -> add([Entry|Acc], From, To, Rest).

% Test if Key is in the list of half open ranges (]
in_ranges(_, []) -> false;
in_ranges(Key, [{From, To, _}|Rest]) ->
	case in_half_open_range(Key, From, To) of
		true -> true;
		_ -> in_ranges(Key, Rest)
	end.

% Expire ranges that have not been added since the last expiration.
expire(Ranges) -> expire(false, [], Ranges).
expire(Expired, Acc, []) -> {Expired, Acc};
expire(Expired, Acc, [{From, To, 1}|Rest]) -> expire(Expired, [{From, To, 0}|Acc], Rest);
expire(_, Acc, [{_, _, 0}|Rest]) -> expire(true, Acc, Rest).
	
% is Value in (From, To)
in_open_range(nil, _, _) -> false;
in_open_range(Value, From, To) ->
	if
		(From < To) ->
			(Value > From) and (Value < To);
		(To < From) ->
			(Value > From) or (Value < To);
		(Value =:= To) -> % and implicitly Value =:= From
			false;
		true -> true
	end.

% is Value in (From, To]
in_half_open_range(nil, _, _) -> false;
in_half_open_range(_, nil, _) -> true;
in_half_open_range(Value, From, To) ->
	if
		(From < To) ->
			(Value > From) and (Value =< To);
		(To < From) ->
			(Value > From) or (Value =< To);
		true -> true
	end.