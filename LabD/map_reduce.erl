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
    io:format(user, "Map phase complete\n", []),
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
    io:format(user, "Map phase complete\n", []),
    Reducers =
	[spawn_reducer(Parent,Reduce,I,Mappeds)
	 || I <- lists:seq(0,R-1)],
    Reduceds =
	[receive {Pid,L} -> L end || Pid <- Reducers],
    io:format(user, "Reduce phase complete\n", []),
    lists:sort(lists:flatten(Reduceds)).

spawn_mapper(Parent,Map,R,Split) ->
    spawn_link(fun() ->
			Mapped = [{erlang:phash2(K2,R),{K2,V2}}
				  || {K,V} <- Split,
				     {K2,V2} <- Map(K,V)],
                        io:format(user, ".", []),
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
                        io:format(user, ".", []),
                        Parent ! {self(),Result} end).

%% 1 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

map_par(Map,M,R,Input) ->
    io:format(user,"Map phase started\n",[]),
    Parent = self(),
    Splits = split_into(M,Input),
    Mappers = [spawn_mapper(Parent,Map,R,Split) || Split <- Splits],
    Mappeds = [receive {Pid,L} -> L end || Pid <- Mappers],
    io:format(user,"Map phase complete\n",[]),
    Mappeds.

reduce_par(Mappeds,Reduce,Is) ->
    io:format(user,"Reduce phase started\n",[]),
    Parent = self(),
    Reducers = [spawn_reducer(Parent,Reduce,I,Mappeds) || I <- Is],
    Reduceds = [receive {Pid,L} -> L end || Pid <- Reducers],
    io:format(user,"Reduce phase complete\n",[]),
    lists:sort(lists:flatten(Reduceds)).


map_reduce_dis(Map,M,Reduce,R,Input) ->
    io:format(user,"Start, number of nodes: ",[]),
    Nodes = nodes() ++ [node()],
    Num_Nodes = length(Nodes),
    print(Num_Nodes),
    Work_Split = split_into(Num_Nodes,Input),
    Zip_Map = lists:zip(Nodes,Work_Split),

    RequestIdsM = [ erpc:send_request(Node,map_reduce,map_par,[Map,M,R,Work]) || {Node,Work} <- Zip_Map],
    Mappedss   = [ erpc:receive_response(RequestId) || RequestId <- RequestIdsM],

    Mappeds = lists:concat(Mappedss),
    io:format(user,"Map phase complete\n",[]),


    Is_Split = split_into(Num_Nodes, lists:seq(0,R-1)),
    Zip_Reduce = lists:zip(Nodes,Is_Split),


    RequestIdsR = [ erpc:send_request(Node,map_reduce,reduce_par,[Mappeds,Reduce,Is]) || {Node,Is} <- Zip_Reduce],
    Reducedss = [ erpc:receive_response(RequestId) || RequestId <- RequestIdsR],


    Reduceds = lists:concat(Reducedss),
    io:format(user,"Reduce phase complete\n",[]),
    lists:sort(lists:flatten(Reduceds)).

%% 2 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

split(N, Xs) when length(Xs) < N -> {Xs,[]};
split(N, Xs) -> lists:split(N,Xs).

wait_for_response(Work, Nodes, Ids, R, F, M)->
    [{RequestId,Node} | NewIds] = Ids,
    case erpc:wait_response(RequestId) of
        {response, Result} -> worker_pool(Work, Nodes ++ [Node], NewIds, R ++ [Result], F, M);
        no_response        -> worker_pool(Work, Nodes, NewIds ++ [{RequestId,Node}], R, F, M)
    end.

worker_pool([],       _,  [], R, _, _) -> R;
worker_pool([],   Nodes, Ids, R, F, Chunk) ->
    wait_for_response([], Nodes, Ids, R, F, Chunk);
worker_pool(Work,    [], Ids, R, F, Chunk) ->
    wait_for_response(Work, [], Ids, R, F, Chunk);
worker_pool(Work, Nodes, Ids, R, F, Chunk) ->
    [Node | Idle] = Nodes,
    {Job, WorkLeft} = split(Chunk,Work),
    Id = erpc:send_request(Node, fun() -> F(Job) end),    %% cast and send msg instead
    NewIds = Ids ++ [{Id,Node}],
    worker_pool(WorkLeft, Idle, NewIds, R, F, Chunk).

%%

map_reduce_pool(Map,M,Reduce,R,Input) ->
    io:format(user,"Start, number of nodes: ",[]),
    Nodes = [node() | nodes()],
    Num_Nodes = length(Nodes),
    print(Num_Nodes),
    FM = fun(W) -> map_par(Map, M, R, W) end,
    Mappedss = worker_pool(Input, Nodes, [], [], FM, M * 4),

    Mappeds = lists:concat(Mappedss),
    io:format(user,"Map phase complete\n",[]),


    Is =  lists:seq(0,R-1),
    FR = fun(W) -> reduce_par(Mappeds, Reduce, W) end,
    Reducedss = worker_pool(Is, Nodes, [], [], FR, R div Num_Nodes),


    Reduceds = lists:concat(Reducedss),
    io:format(user,"Reduce phase complete\n",[]),
    lists:sort(lists:flatten(Reduceds)).

%% 2.1 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
worker_pool2(Work,F,Chunk) -> worker_pool21(Work, F, Chunk, [node() | nodes()], [], []).


wait_for_response2(Work, F, Chunk, Nodes, Refs, Result) ->
    receive
        {result, {Ref,Node}, R} ->                                                                          %% todo (lists:member({Ref,Node}, Refs))
            worker_pool21(Work, F, Chunk, Nodes ++ [Node], Refs -- [{Ref,Node}], Result ++ [R]);

        X -> print({"error response",X})
    end.

worker_pool21([], _, _, _, [], Result) -> Result;
worker_pool21([], F, Chunk, Nodes, Refs, Result) ->
    wait_for_response2([], F, Chunk, Nodes, Refs, Result);
worker_pool21(Work, F, Chunk, [], Refs, Result) ->
    wait_for_response2(Work, F, Chunk, [], Refs, Result);
worker_pool21(Work, F, Chunk, Nodes, Refs, Result) ->
    [Node | Idle] = Nodes,
    {Job, WorkLeft} = split(Chunk,Work),
    Parent = self(),
    Ref = {make_ref(), Node},
    erpc:cast(Node, fun() -> Parent ! {result, Ref, F(Job)} end),
    NewRefs = Refs ++ [Ref],
    worker_pool21(WorkLeft, F, Chunk, Idle, NewRefs, Result).


map_reduce_pool2(Map,M,Reduce,R,Input) ->
    io:format(user,"Start, number of nodes: ",[]),
    Nodes = [node() | nodes()],
    Num_Nodes = length(Nodes),
    print(Num_Nodes),
    FM = fun(W) -> map_par(Map, M, R, W) end,
    Mappedss = worker_pool2(Input, FM, M *4),

    Mappeds = lists:concat(Mappedss),
    io:format(user,"Map phase complete\n",[]),


    Is =  lists:seq(0,R-1),
    FR = fun(W) -> reduce_par(Mappeds, Reduce, W) end,
    Reducedss = worker_pool2(Is, FR, R),


    Reduceds = lists:concat(Reducedss),
    io:format(user,"Reduce phase complete\n",[]),
    lists:sort(lists:flatten(Reduceds)).


%% 2.2 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



worker_pool3(F, Args) ->
    Ref = make_ref(),
    Nodes = [node() | nodes()],
    [ monitor_node(Node, true) || Node <- Nodes],
    Main = node(),
    [spawn_link(Node, fun()-> register(worker,self()),  worker(F, {main,Main},Ref) end) || Node <- Nodes],
    Result = wait_response(Args, Ref, [], []),
    [{worker,Node} ! done || Node <- Nodes],
    flush_msg(),
    Result.


flush_msg()->
    receive
        {employ_me,_,_,_} -> flush_msg()
    after 0 -> ok
    end.

wait_response([], _ ,[], Results) -> Results;
wait_response([],Id,Refs, Results) ->
    receive
        {result, Ref, Result} ->
            RefsNew = lists:keydelete(Ref,2,Refs),
            wait_response([], Id, RefsNew, [Result | Results])
    end;
wait_response(Args, Id, Refs, Results) ->
    receive
        {employ_me, Id , Node, Pid} ->
            [Arg | ArgsNew] = Args,
            Ref = make_ref(),
            Pid ! {job, Arg ,Ref},
            wait_response(ArgsNew, Id, [{Node, Ref, Arg} | Refs], Results);

        {result, Ref, Result} ->
            RefsNew = lists:keydelete(Ref,2,Refs),
            wait_response(Args, Id, RefsNew, [Result | Results] )
    end.


worker(F, Main, Id) ->
    Main ! {employ_me, Id, node() ,{worker, node()}},
    receive
        {job, Arg, Ref} ->
            Result = F(Arg),
            Main ! {result, Ref, Result},
            worker(F, Main, Id);
        done -> unregister(worker),ok
    end.




map_reduce_pool3(Map,M,Reduce,R,Input) ->
    io:format(user,"Start, number of nodes: ",[]),
    Nodes = [node() | nodes()],
    Num_Nodes = length(Nodes),
    print(Num_Nodes),

    register(main, self()),
    MChunk = 4*M,
    InputChunk = split_into(length(Input) div MChunk, Input),
    FM = fun(W) -> map_par(Map, M, R, W) end,
    Mappedss = worker_pool3(FM, InputChunk),

    Mappeds = lists:concat(Mappedss),
    io:format(user,"Map phase complete\n",[]),


    Is =  lists:seq(0,R-1),
    IChunk = 4,
    IsChunk = split_into(R div IChunk, Is),
    FR = fun(W) -> reduce_par(Mappeds, Reduce, W) end,
    Reducedss = worker_pool3(FR, IsChunk),


    Reduceds = lists:concat(Reducedss),
    unregister(main),
    io:format(user,"Reduce phase complete\n",[]),
    lists:sort(lists:flatten(Reduceds)).


%% 2.4 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

worker_pool4(F, Args) ->
    Ref = make_ref(),
    Nodes = [node() | nodes()],
    [ monitor_node(Node, true) || Node <- Nodes],
    Main = node(),
    [spawn_link(Node, fun()-> register(worker,self()), nodeWorker(F, {main,Main},Ref) end) || Node <- Nodes],
    Result = wait_response(Args, Ref, [], []),
    [{worker,Node} ! done || Node <- Nodes],
    flush_msg(),
    Result.

map_seq(Map,Input) ->
    io:format(user,".",[]),
    [{K2,V2}
	      || {K,V} <- Input,
		 {K2,V2} <- Map(K,V)].


reduce_seq(Reduce,I,Mappeds) ->
    io:format(user,".",[]),
    Inputs = [KV
	      || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
         reduce_seq(Reduce,Inputs).



nodeWorker(F, Main, Id) ->
    Parent = self(),
    N = erlang:system_info(schedulers),
    Workers = [spawn_link(threadWorker(F,Parent)) || _ <- lists:seq(1,N)],
    work(F,Main,Id,Workers).


zip(_,[]) -> [];
zip([],_) -> [];
zip([X|Xs],[Y|Ys]) -> [{X,Y} | zip(Xs,Ys)].


work(F, Main, Id, Workers) ->
    Main ! {employ_me, Id, node() ,{worker, node()}},
    receive
        {job, Arg, Ref} ->

            Args = split_into(length(Workers),Arg),
            Zip = zip(Workers,Args),
            [ Pid ! {job, Job } || {Pid, Job} <- Zip],
            Result = [receive {result, R} -> R end || _ <- Workers],

            Main ! {result, Ref, Result},
            worker(F, Main, Id);
        done ->
             unregister(worker),
             [Pid ! done || Pid <- Workers],
             ok
    end.

threadWorker(F,Parent) ->
    receive
        {job, Arg} ->
            Parent ! {result, F(Arg)},
            threadWorker(F,Parent);
        done -> ok
    end.

map_reduce_pool4(Map,M,Reduce,R,Input) ->
    io:format(user,"Start, number of nodes: ",[]),
    Nodes = [node() | nodes()],
    Num_Nodes = length(Nodes),
    print(Num_Nodes),

    register(main, self()),
    MChunk = 4*M,
    InputChunk = split_into(length(Input) div MChunk, Input),
    FM = fun(W) -> map_seq(Map, W) end,
    Mappedss = worker_pool4(FM, InputChunk),

    Mappeds = lists:concat(Mappedss),
    io:format(user,"Map phase complete\n",[]),


    Is =  lists:seq(0,R-1),
    IChunk = 4,
    IsChunk = split_into(R div IChunk, Is),
    FR = fun(Ws) -> [reduce_seq(Mappeds, Reduce, W) || W <- Ws] end,
    Reducedss = worker_pool4(FR, IsChunk),


    Reduceds = lists:concat(Reducedss),
    unregister(main),
    io:format(user,"Reduce phase complete\n",[]),
    lists:sort(lists:flatten(Reduceds)).
%% 3 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



worker_pool_fault(F, Args) ->
    Ref = make_ref(),
    Nodes = [node() | nodes()],
    [ monitor_node(Node, true) || Node <- Nodes],
    Main = node(),
    [ spawn(Node, fun()-> register(worker,self()), worker(F, {main,Main},Ref) end) || Node <- Nodes],
    Result = wait_response_fault(Args, Ref, [], []),
    [{worker,Node} ! done || Node <- Nodes],
    flush_msg(),
    Result.


nodedown(Node, Args, Id, Refs, Results) ->
        print("hej"),
        print(Node),
        case lists:keyfind(Node, 1, Refs) of
            false ->
                 wait_response_fault(Args, Id ,Refs, Results);
            {Node,Ref,Arg} ->
                print("då"),
                 RefsNew = lists:keydelete(Ref, 2, Refs),
                 nodedown(Node, [Arg | Args], Id, RefsNew, Results)
                 %wait_response_fault([Arg | Args], Id ,RefsNew, Results)
        end.


wait_response_fault([], _ ,[], Results) -> Results;
wait_response_fault([],Id,Refs, Results) ->
    receive
        {result, Ref, Result} ->
            RefsNew = lists:keydelete(Ref,2,Refs),
            wait_response_fault([], Id, RefsNew, [Result | Results]);

        {nodedown, Node} -> nodedown(Node, [], Id, Refs, Results);
        {noconnection, Node} -> nodedown(Node, [], Id, Refs, Results)

    end;
wait_response_fault(Args, Id, Refs, Results) ->
    receive
        {employ_me, Id , Node, Pid} ->
            [Arg | ArgsNew] = Args,
            Ref = make_ref(),
            Pid ! {job, Arg ,Ref},
            wait_response_fault(ArgsNew, Id, [{Node, Ref, Arg} | Refs], Results);

        {result, Ref, Result} ->
            RefsNew = lists:keydelete(Ref,2,Refs),
            wait_response_fault(Args, Id, RefsNew, [Result | Results] );

        {nodedown, Node} -> nodedown(Node, Args, Id, Refs, Results);
        {noconnection, Node} -> nodedown(Node, Args, Id, Refs, Results)

    end.




map_reduce_fault(Map,M,Reduce,R,Input) ->
    io:format(user,"Start, number of nodes: ",[]),
    Nodes = [node() | nodes()],
    Num_Nodes = length(Nodes),
    print(Num_Nodes),

    register(main, self()),
    MChunk = 4*M,
    InputChunk = split_into(length(Input) div MChunk, Input),
    FM = fun(W) -> map_par(Map, M, R, W) end,
    Mappedss = worker_pool_fault(FM, InputChunk),

    Mappeds = lists:concat(Mappedss),
    io:format(user,"Map phase complete\n",[]),


    Is =  lists:seq(0,R-1),
    IChunk = 4,
    IsChunk = split_into(R div IChunk, Is),
    FR = fun(W) -> reduce_par(Mappeds, Reduce, W) end,
    Reducedss = worker_pool_fault(FR, IsChunk),


    Reduceds = lists:concat(Reducedss),
    unregister(main),
    io:format(user,"Reduce phase complete\n",[]),
    lists:sort(lists:flatten(Reduceds)).




%% Misc %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

print(M) ->
    io:format(user,"~p ~n~n",[M]).

stop() ->
    receive
        start -> ok
    end.
