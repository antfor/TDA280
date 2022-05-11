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

%% 1 -----------------------------------------------------------------------
map_reduce_dist(Map,M,Reduce,R,Input) ->
    Nodes = nodes() ++ [node()],
    Node_Amount = length (Nodes),
    Node_Splits = split_into(Node_Amount, Input),
    
    Parent = self(),

    Mappers = [spawn_mapper_node(Parent, Node, Map, M, R, Split)
                || {Node, Split} <- lists:zip (Nodes, Node_Splits)],
    Mappeds = lists:concat([receive {Pid, L} -> L end || Pid <- Mappers]),
    io:format("Map phase complete\n"),
    
    Hash_Splits = split_into(Node_Amount, lists:seq(0, R - 1)),
    Reducers = [spawn_reducer_node(Parent, Node, Reduce, Mappeds, Split)
                || {Node, Split} <- lists:zip (Nodes, Hash_Splits)],
    Reduceds = lists:concat([receive {Pid, L} -> L end || Pid <- Reducers]),
    io:format("Reduce phase complete\n"),
    lists:sort(lists:flatten(Reduceds)).

spawn_mapper_node(Parent, Node, Map, M, R, Input) ->
    spawn_link(Node,
        fun () ->
            NodePID = self(),
            Splits = split_into(M, Input),
            Mappers = [spawn_mapper(NodePID, Map, R, Split) || Split <- Splits],
            Mappeds = [receive {Pid, L} -> L end || Pid <- Mappers],
            Parent ! {NodePID, Mappeds}
        end).

spawn_reducer_node(Parent, Node, Reduce, Mappeds, Hashes) ->
    spawn_link(Node,
        fun () ->
            NodePID = self(),
            Reducers = [spawn_reducer(NodePID, Reduce, Hash, Mappeds) || Hash <- Hashes],
            Reduceds = [receive {Pid, L} -> L end || Pid <- Reducers],
            Parent ! {NodePID, Reduceds}
        end).

%% 2 -----------------------------------------------------------------------
map_reduce_dist_pool(Map,M,Reduce,R,Input) ->
    Nodes = nodes() ++ [node()],
    Worker_Pool = start_worker_pool(Nodes),

    Splits = split_into(M, Input),
    Map_Work = [fun() -> map_dist(Map, R, Split) end || Split <- Splits],
    Worker_Pool ! {do_work, Map_Work},
    Mappeds = receive {retrieve_res, Res} -> Res end,
    io:format("Map phase complete\n"),
    Mappeds.

map_dist(Map, R, Split) ->
    Mapped = [{erlang:phash2(K2, R), {K2, V2}}
                || {K, V} <- Split,
                   {K2, V2} <- Map(K, V)],
    io:format("."),
    Mapped.

start_worker_pool(Nodes) ->
    Parent = self(),
    spawn_link(
        fun() ->
            Node_PIDs = [spawn_node_worker(Node) || Node <- Nodes],
            worker_pool(passive, Parent, Node_PIDs, [])
        end).

worker_pool(passive, Parent, Node_PIDs, []) ->
    receive
        {do_work, Work} ->
            [Node ! {start} || Node <- Node_PIDs],
            worker_pool(active, Parent, Node_PIDs, Work)
    end;
worker_pool(active, Parent, Node_PIDs, [W|Ws]) ->
    receive
        {request_work, Node_PID} ->
            Node_PID ! {do_work, W},
            worker_pool(active, Parent, Node_PIDs, Ws)
    end;
worker_pool(active, Parent, Node_PIDs, []) ->
    [receive {request_work, Node_PID} -> Node_PID ! {no_more_work} end 
              || Node_PID <- Node_PIDs],
    Result = lists:concat([receive {result, Node_PID, Work_Res} -> Work_Res end || Node_PID <- Node_PIDs]),
    Parent ! {retrieve_res, Result},
    worker_pool(passive, Parent, Node_PIDs, []).

spawn_node_worker(Node) ->
    Parent = self(),
    spawn_link(Node,
            fun() ->
                Thread_Amount = erlang:system_info(schedulers - 1),
                Threads = [spawn_link(Node, fun() -> thread_worker(self()) end) 
                            || lists:seq(1, Thread_Amount)],
                node_worker(passive, Parent, Node, Threads, Threads, [])
            end
        ).

node_worker(passive, Parent, Node, Threads, All_Threads, Work_Res) ->
    receive
        {start} ->
            node_worker(active, Parent, Node, Threads, All_Threads, Work_Res)
    end;
node_worker(active, Parent, Node, [], All_Threads, Work_Res) ->
    receive 
        {work_done, T, Ref, Res} ->
            node_worker(active, Parent, Node, [T], All_Threads, [Res|Work_Res])
    end;
node_worker(active, Parent, Node, [T|Ts], All_Threads, Work_Res) ->
    Parent ! {request_work, self()},
    receive
        {do_work, Work} ->
            Ref = make_ref(),
            T ! {do_work, Ref, Work},
            node_worker(active, Parent, Node, Ts, All_Threads, Work_Res);
        {no_more_work} -> 
            node_worker(collecting_res, Parent, Node, [T|Ts], All_Threads, Work_Res)
    end;
node_worker(collecting_res, Parent, Node, Threads, All_Threads, Work_Res) ->
    if 
        length(Threads) == length(All_Threads) ->
            Parent ! {result, Node, Work_Res},
            node_worker(passive, Parent, Node, Threads, All_Threads, []);
        true ->
            receive 
                {work_done, T, Ref, Res} ->
                    node_worker(collecting_res, Parent, Node, [T|Threads], All_Threads, [Res|Work_Res])
            end
    end.    
            
thread_worker(Master_Thread) ->
    receive
        {do_work, Ref, Work} ->
            Res = Work(),
            Master_Thread ! {work_done, self(), Ref, Res},
            thread_worker(Master_Thread)
    end.