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

%% MAP REDUCE SEQ
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

%% MAP REDUCE PAR

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

%% DISTRIBUTED MAP REDUCE
map_reduce_dist(Map, M, Reduce, R, Input) ->
    Nodes  = nodes(), %% Use all nodes except this one
    All_Splits = split_into(M, Input),
    Dist_Splits = split_into(length(Nodes), All_Splits),
    [spawn_link(Node, fun() -> spawn_mapper_dist(node(), Map, R, Splits) end) 
        || {Node, Splits} <- lists:zip(Nodes, Dist_Splits)],
    Mappeds = lists:concat([receive {Node,Mapped} -> Mapped end || Node <- Nodes]),
    io:format("Map phase complete\n"),

    Hash_Splits = split_into(length(nodes), lists:seq(0, R-1)),
    [spawn_link(Node, fun() -> spawn_reduce_dist(node(), Reduce, Indices, Mappeds) end) 
        || {Node, Indices} <- lists:zip(Nodes, Hash_Splits)],
    Reduceds = lists:concat([receive {Node,Reduced} -> Reduced end || Node <- Nodes]),
    io:format("Reduce phase complete\n"),
    lists:sort(lists:flatten(Reduceds)).

spawn_reduce_dist(Master,Reduce,Indices,Mappeds) ->
    Parent = self(),
    Reducers =
    [spawn_reducer(Parent,Reduce,I,Mappeds) 
	    || I <- Indices],
    Reduceds = 
        [receive {Pid,L} -> L end || Pid <- Reducers],
    Master ! {node(), Reduceds}.
    

spawn_mapper_dist(Master, Map, R, Splits) ->
    Parent = self(),
    Mappers = 
	    [spawn_mapper(Parent,Map,R,Split)
	        || Split <- Splits],
    Mappeds = 
	    [receive {Pid,L} -> L end || Pid <- Mappers],
    Master ! {node(), Mappeds}.