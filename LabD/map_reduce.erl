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

    Map_Splits = split_into(M, Input),
    Map_Work = [fun() -> map_dist(Map, R, Split) end || Split <- Map_Splits],

    Worker_Pool ! {do_work, Map_Work},
    Mappeds = receive {retrieve_res, Ma} -> Ma end,
    io:format("Map phase complete\n"),
    
    Reduce_Splits = [[KV || Mapped <- Mappeds,
                            {J, KVs} <-Mapped,
                            I == J,
                            KV <- KVs] 
                        || I <- lists:seq(0, R - 1)],
    Reduce_Work = [fun() -> reduce_dist(Reduce, Split) end || Split <- Reduce_Splits],

    Worker_Pool ! {do_work, Reduce_Work},
    Reduceds = receive {retrieve_res, Re} -> Re end,
    io:format("Reduce phase complete\n"),
    lists:sort(lists:flatten(Reduceds)).

map_dist(Map, R, Split) ->
    Mapped = [{erlang:phash2(K2,R),{K2,V2}}
				  || {K,V} <- Split,
				     {K2,V2} <- Map(K,V)],
    io:format(user, ".", []),
	group(lists:sort(Mapped)).

reduce_dist(Reduce, Split) ->
    Reduced = reduce_seq(Reduce, Split),
    io:format(user, ".", []),
    Reduced.

start_worker_pool(Nodes) ->
    Main_Proc = self(),
    spawn_link(
        fun() ->
            Node_Procs = [spawn_node_worker(Node) || Node <- Nodes],
            worker_pool(passive, Main_Proc, Node_Procs, [])
        end).

worker_pool(passive, Main_Proc, Node_Procs, []) ->
    receive
        {do_work, Work} ->
            [Node_Proc ! {start} || Node_Proc <- Node_Procs],
            worker_pool(active, Main_Proc, Node_Procs, Work)
    end;
worker_pool(active, Main_Proc, Node_Procs, [W|Ws]) ->
    receive
        {request_work, Node_Proc} ->
            Node_Proc ! {do_work, W},
            worker_pool(active, Main_Proc, Node_Procs, Ws)
    end;
worker_pool(active, Main_Proc, Node_Procs, []) ->
    [receive {request_work, Node_Proc} -> Node_Proc ! {no_more_work} end 
              || Node_Proc <- Node_Procs],
    Result = lists:concat([receive {result, Node_Proc, Work_Res} -> Work_Res end || Node_Proc <- Node_Procs]),
    Main_Proc ! {retrieve_res, Result},
    worker_pool(passive, Main_Proc, Node_Procs, []).

spawn_node_worker(Node) ->
    Worker_Pool_Proc = self(),
    spawn(Node,
            fun() ->
                Node_Proc = self(),
                Worker_Amount = erlang:system_info(schedulers), % One master thread and one thread left to os
                Threads = [spawn_link(Node, fun() -> thread_worker(Node_Proc) end) 
                            || _ <- lists:seq(1, Worker_Amount)],
                node_worker(passive, Worker_Pool_Proc, Threads, Threads, [])
            end
        ).

node_worker(passive, Worker_Pool_Proc, Threads, All_Threads, Work_Res) ->
    receive
        {start} ->
            node_worker(active, Worker_Pool_Proc, Threads, All_Threads, Work_Res)
    end;
node_worker(active, Worker_Pool_Proc, [], All_Threads, Work_Res) ->
    receive 
        {work_done, T, Res} ->
            node_worker(active, Worker_Pool_Proc, [T], All_Threads, [Res|Work_Res])
    end;
node_worker(active, Worker_Pool_Proc, [T|Ts], All_Threads, Work_Res) ->
    Worker_Pool_Proc ! {request_work, self()},
    receive
        {do_work, Work} ->
            T ! {do_work, Work},
            node_worker(active, Worker_Pool_Proc, Ts, All_Threads, Work_Res);
        {no_more_work} ->
            node_worker(collecting_res, Worker_Pool_Proc, [T|Ts], All_Threads, Work_Res)
    end;
node_worker(collecting_res, Worker_Pool_Proc, Threads, All_Threads, Work_Res) ->
    if 
        length(Threads) == length(All_Threads) ->
            Worker_Pool_Proc ! {result, self(), Work_Res},
            node_worker(passive, Worker_Pool_Proc, Threads, All_Threads, []);
        true ->
            receive 
                {work_done, T, Res} ->
                    node_worker(collecting_res, Worker_Pool_Proc, [T|Threads], All_Threads, [Res|Work_Res])
            end
    end.    
            
thread_worker(Owner_Proc) ->
    receive
        {do_work, Work} ->
            Res = Work(),
            Owner_Proc ! {work_done, self(), Res},
            thread_worker(Owner_Proc)
    end.

%% 3 -----------------------------------------------------------------------
map_reduce_dist_pool_f(Map,M,Reduce,R,Input) ->
    Nodes = nodes() ++ [node()],
    Worker_Pool = start_worker_pool_f(Nodes),

    Map_Splits = split_into(M, Input),
    Map_Work = [fun() -> map_dist(Map, R, Split) end || Split <- Map_Splits],

    Mappeds = execute_work(Worker_Pool, Map_Work),
    io:format("Map phase complete\n"),
    
    Reduce_Splits = [[KV || Mapped <- Mappeds,
                            {J, KVs} <-Mapped,
                            I == J,
                            KV <- KVs] 
                        || I <- lists:seq(0, R - 1)],
    Reduce_Work = [fun() -> reduce_dist(Reduce, Split) end || Split <- Reduce_Splits],

    Reduceds = execute_work(Worker_Pool, Reduce_Work),
    io:format("Reduce phase complete\n"),
    lists:sort(lists:flatten(Reduceds)).

execute_work(Worker_Pool, Work) ->
    execute_work(Worker_Pool, Work, []).

execute_work(Worker_Pool, Work, Curr_Result) ->
    Worker_Pool ! {do_work, Work},
    {Result, Leftover_Work} = receive {retrieve_res, Res} -> Res end,
    Result_Upd = lists:append(Result, Curr_Result),
    case Leftover_Work of
        [] -> Result_Upd;
        _  -> execute_work(Worker_Pool, Leftover_Work, Result_Upd)
    end.

start_worker_pool_f(Nodes) ->
    Main_Proc = self(),
    spawn(
        fun() ->
            Node_Procs = [{Node, spawn_node_worker_f(Node)} || Node <- Nodes],
            [monitor_node(Node, true) || Node <- Nodes],
            worker_pool_f(passive, Main_Proc, Node_Procs, [], [])
        end).

worker_pool_f(passive, Main_Proc, Node_Procs, [], []) ->
    receive
        {do_work, Work} ->
            [Node_PID ! {start} || {_, Node_PID} <- Node_Procs],
            Sent_Work = [{Node_PID, []} || {_, Node_PID} <- Node_Procs],
            worker_pool_f(active, Main_Proc, Node_Procs, Sent_Work, Work)
    end;
worker_pool_f(active, Main_Proc, Node_Procs, Sent_Work, [W|Ws]) ->
    receive
        {request_work, Node_PID} ->
            Node_PID ! {do_work, W},
            {_, Node_Work_List} = lists:keyfind(Node_PID, 1, Sent_Work),
            Sent_Work_Upd = 
                lists:keyreplace(Node_PID, 1, Sent_Work, {Node_PID, [W|Node_Work_List]}),
            worker_pool_f(active, Main_Proc, Node_Procs, Sent_Work_Upd, Ws);
        
        {nodedown, Node} ->
            {_, Node_PID} = lists:keyfind(Node, 1, Node_Procs),
            {_, Work_List} = lists:keyfind(Node_PID, 1, Sent_Work),
            Work_Upd = lists:append(Work_List, [W|Ws]),
            Sent_Work_Upd = lists:keydelete(Node_PID, 1, Sent_Work),
            Node_Procs_Upd = lists:keydelete(Node_PID, 2, Node_Procs),
            worker_pool_f(active, Main_Proc, Node_Procs_Upd, Sent_Work_Upd, Work_Upd)
    end;
worker_pool_f(active, Main_Proc, Node_Procs, Sent_Work, []) ->
    {Res, Leftover, Node_Procs_Upd} = get_node_results(Node_Procs, Sent_Work),
    Main_Proc ! {retrieve_res, {Res, Leftover}},
    worker_pool_f(passive, Main_Proc, Node_Procs_Upd, [], []).

get_node_results (Node_Procs, Sent_Work) ->
    get_node_results(Node_Procs, Node_Procs, [], Sent_Work).

get_node_results([], All_Node_Procs, Result, Sent_Work) ->
    Leftover_Work = lists:concat([Work || {_, Work} <- Sent_Work]),
    {Result, Leftover_Work, All_Node_Procs};
get_node_results([Node_Proc|Node_Procs], All_Node_Procs, Result, Sent_Work) ->
    {Node_Name, Node_PID} = Node_Proc,
    receive 
        {request_work, Node_PID} -> Node_PID ! {no_more_work},
            receive
                {result, Node_PID, Work_Res} -> 
                    Result_Upd = lists:append(Work_Res, Result),
                    Sent_Work_Upd = lists:keydelete(Node_PID, 1, Sent_Work),
                    get_node_results(Node_Procs, All_Node_Procs, Result_Upd, Sent_Work_Upd);
                {nodedown, Node_Name} -> 
                    All_Node_Procs_Upd = lists:keydelete(Node_Name, 1, All_Node_Procs),
                    get_node_results(Node_Procs, All_Node_Procs_Upd, Result, Sent_Work)
            end;
        {nodedown, Node_Name} -> 
            All_Node_Procs_Upd = lists:keydelete(Node_Name, 1, All_Node_Procs),
            get_node_results(Node_Procs, All_Node_Procs_Upd, Result, Sent_Work)
    end.

spawn_node_worker_f(Node) ->
    Worker_Pool_Proc = self(),
    spawn(Node,
            fun() ->
                Node_Proc = self(),
                Worker_Amount = erlang:system_info(schedulers),
                Threads = [spawn_link(Node, fun() -> thread_worker_f(Node_Proc) end) 
                            || _ <- lists:seq(1, Worker_Amount)],
                node_worker_f(passive, Worker_Pool_Proc, Threads, Threads, [])
            end
        ).

node_worker_f(passive, Worker_Pool_Proc, Threads, All_Threads, Work_Res) ->
    receive
        {start} ->
            node_worker_f(active, Worker_Pool_Proc, Threads, All_Threads, Work_Res)
    end;
node_worker_f(active, Worker_Pool_Proc, [], All_Threads, Work_Res) ->
    receive 
        {work_done, T, Res} ->
            node_worker_f(active, Worker_Pool_Proc, [T], All_Threads, [Res|Work_Res])
    end;
node_worker_f(active, Worker_Pool_Proc, [T|Ts], All_Threads, Work_Res) ->
    Worker_Pool_Proc ! {request_work, self()},
    receive
        {do_work, Work} ->
            T ! {do_work, Work},
            node_worker_f(active, Worker_Pool_Proc, Ts, All_Threads, Work_Res);
        {no_more_work} ->
            node_worker_f(collecting_res, Worker_Pool_Proc, [T|Ts], All_Threads, Work_Res)
    end;
node_worker_f(collecting_res, Worker_Pool_Proc, Threads, All_Threads, Work_Res) ->
    if 
        length(Threads) == length(All_Threads) ->
            Worker_Pool_Proc ! {result, self(), Work_Res},
            node_worker_f(passive, Worker_Pool_Proc, Threads, All_Threads, []);
        true ->
            receive 
                {work_done, T, Res} ->
                    node_worker_f(collecting_res, Worker_Pool_Proc, [T|Threads], All_Threads, [Res|Work_Res])
            end
    end.    
            
thread_worker_f(Owner_Proc) ->
    receive
        {do_work, Work} ->
            Res = Work(),
            Owner_Proc ! {work_done, self(), Res},
            thread_worker_f(Owner_Proc)
    end.