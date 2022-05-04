%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This is a very simple implementation of map-reduce, in both
%% sequential and parallel versions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(map_reduce).
-compile([export_all,nowarn_export_all]).

%% We begin with a simple sequential implementation, just to define
%% the semantics of map-reduce.

%% The input is a collection of key-value pairs. The map function maps
%% each key value pair to a list of key-value pairs. The reduce
%% function is then applied to each key and list of corresponding
%% values, and generates in turn a list of key-value pairs. These are
%% the result.

map_reduce_seq(Map,Reduce,Input) ->
    Mapped = [{K2,V2}
	      || {K,V} <- Input,
		 {K2,V2} <- Map(K,V)],
    io:format("Map phase complete\n"),
    reduce_seq(Reduce,Mapped).

reduce_seq(Reduce,KVs) ->
    [KV || {K,Vs} <- group(lists:sort(KVs)),
	   KV <- Reduce(K,Vs)].

group([]) ->
    [];
group([{K,V}|Rest]) ->
    group(K,[V],Rest).

group(K,Vs,[{K,V}|Rest]) ->
    group(K,[V|Vs],Rest);
group(K,Vs,Rest) ->
    [{K,lists:reverse(Vs)}|group(Rest)].

map_reduce_par(Map,M,Reduce,R,Input) ->
    Parent = self(),
    Splits = split_into(M,Input),
    Mappers =
	[spawn_mapper(Parent,Map,R,Split)
	 || Split <- Splits],
    Mappeds =
	[receive {Pid,L} -> L end || Pid <- Mappers],
    io:format("Map phase complete\n"),
    Reducers =
	[spawn_reducer(Parent,Reduce,I,Mappeds)
	 || I <- lists:seq(0,R-1)],
    Reduceds =
	[receive {Pid,L} -> L end || Pid <- Reducers],
    io:format("Reduce phase complete\n"),
    lists:sort(lists:flatten(Reduceds)).

spawn_mapper(Parent,Map,R,Split) ->
    spawn_link(fun() ->
			Mapped = [{erlang:phash2(K2,R),{K2,V2}}
				  || {K,V} <- Split,
				     {K2,V2} <- Map(K,V)],
                        io:format("."),
			Parent ! {self(),group(lists:sort(Mapped))}
		end).

split_into(N,L) ->
    split_into(N,L,length(L)).

split_into(1,L,_) ->
    [L];
split_into(N,L,Len) ->
    {Pre,Suf} = lists:split(Len div N,L),
    [Pre|split_into(N-1,Suf,Len-(Len div N))].

spawn_reducer(Parent,Reduce,I,Mappeds) ->
    Inputs = [KV
	      || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
    spawn_link(fun() -> Result = reduce_seq(Reduce,Inputs),
                        io:format("."),
                        Parent ! {self(),Result} end).

%% 1 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

map_par(Map,M,R,Input) ->
    io:format(user,"Map phase started\n",[]),
    Parent = self(),
    Splits = split_into(M,Input),
    Mappers =
	[spawn_mapper(Parent,Map,R,Split) || Split <- Splits],
    Mappeds = [receive {Pid,L} -> L end || Pid <- Mappers],
    io:format(user,"Map phase complete\n",[]),
    Mappeds.

reduce_par(Mappeds,Reduce,Is) ->
    io:format(user,"Reduce phase started\n",[]),
    Parent = self(),
    Reducers =
	[spawn_reducer(Parent,Reduce,I,Mappeds)
	 || I <- Is],
    Reduceds =
	[receive {Pid,L} -> L end || Pid <- Reducers],
    io:format(user,"Reduce phase complete\n",[]),
    lists:sort(lists:flatten(Reduceds)).


map_reduce_dis(Map,M,Reduce,R,Input) ->
    Nodes = nodes() ++ [node()],
    Num_Nodes = length(Nodes),
    print(Num_Nodes),
    Work_Split = split_into(Num_Nodes,Input),
    Zip_Map = lists:zip(Nodes,Work_Split),

    Mappedss =
        [ rpc:call(Node,map_reduce,map_par,[Map,M,R,Work]) || {Node,Work} <- Zip_Map], % M/Num_Nodes

    Mappeds = lists:concat(Mappedss),
    io:format(user,"Map phase complete\n",[]),


    Is_Split = split_into(Num_Nodes, lists:seq(0,R-1)),
    Zip_Reduce = lists:zip(Nodes,Is_Split),
    Reducedss =
        [ rpc:call(Node,map_reduce,reduce_par,[Mappeds,Reduce,Is]) || {Node,Is} <- Zip_Reduce],

    Reduceds = lists:concat(Reducedss),
    io:format(user,"Reduce phase complete\n",[]),
    lists:sort(lists:flatten(Reduceds)).

%% 2 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 3 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Misc %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

print(M) ->
    io:format(user,"~p ~n~n",[M]).

stop() ->
    receive
        start -> ok
    end.
