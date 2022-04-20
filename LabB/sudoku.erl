-module(sudoku).
%-include_lib("eqc/include/eqc.hrl").
-compile(export_all).

%% %% generators

%% matrix(M,N) ->
%%     vector(M,vector(N,nat())).

%% matrix transpose

transpose([Row]) ->
    [[X] || X <- Row];
transpose([Row|M]) ->
    [[X|Xs] || {X,Xs} <- lists:zip(Row,transpose(M))].

%% prop_transpose() ->
%%     ?FORALL({M,N},{nat(),nat()},
%% 	    ?FORALL(Mat,matrix(M+1,N+1),
%% 		    transpose(transpose(Mat)) == Mat)).

%% map a matrix to a list of 3x3 blocks, each represented by the list
%% of elements in row order

triples([A,B,C|D]) ->
    [[A,B,C]|triples(D)];
triples([]) ->
    [].

blocks(M) ->
    Blocks = [triples(X) || X <- transpose([triples(Row) || Row <- M])],
    lists:append(
      lists:map(fun(X)->
			lists:map(fun lists:append/1, X)
		end,
		Blocks)).

unblocks(M) ->
    lists:map(
      fun lists:append/1,
      transpose(
	lists:map(
	  fun lists:append/1,
	  lists:map(
	    fun(X)->lists:map(fun triples/1,X) end,
	    triples(M))))).

%% prop_blocks() ->
%%     ?FORALL(M,matrix(9,9),
%% 	    unblocks(blocks(M)) == M).

%% decide whether a position is safe

entries(Row) ->
    [X || X <- Row,
	  1 =< X andalso X =< 9].

safe_entries(Row) ->
    Entries = entries(Row),
    lists:sort(Entries) == lists:usort(Entries).

safe_rows(M) ->
    lists:all(fun safe_entries/1,M).

safe(M) ->
    safe_rows(M) andalso
	safe_rows(transpose(M)) andalso
	safe_rows(blocks(M)).

%% fill blank entries with a list of all possible values 1..9

fill(M) ->
    Nine = lists:seq(1,9),
    [[if 1=<X, X=<9 ->
	      X;
	 true ->
	      Nine
      end
      || X <- Row]
     || Row <- M].

%% refine entries which are lists by removing numbers they are known
%% not to be

refine(M) ->
    NewM =
	refine_rows(
	  transpose(
	    refine_rows(
	      transpose(
		unblocks(
		  refine_rows(
		    blocks(M))))))),
    %% No use parallelizing NewM since we still need to wait for its value
    if M==NewM ->
	    M;
       true ->
	    refine(NewM)
    end.

refine_rows(M) ->
    %%Status = pool_send_work(fun() -> catch lists:map(fun refine_row/1, M) end),
    %%pool_wait_for_value(Status).

    %map_pool_chunk(9, fun(X) -> catch refine_row(X) end, M).
    
    lists:map(fun refine_row/1, M).

refine_row(Row) ->
    Entries = entries(Row),
    NewRow =
	[if is_list(X) ->
		 case X--Entries of
		     [] ->
			 exit(no_solution);
		     [Y] ->
			 Y;
		     NewX ->
			 NewX
		 end;
	    true ->
		 X
	 end
	 || X <- Row],
    NewEntries = entries(NewRow),
    %% check we didn't create a duplicate entry
    case length(lists:usort(NewEntries)) == length(NewEntries) of
	true ->
	    NewRow;
	false ->
	    exit(no_solution)
    end.

is_exit({'EXIT',_}) ->
    true;
is_exit(_) ->
    false.

%% is a puzzle solved?

solved(M) ->
    lists:all(fun solved_row/1,M).

solved_row(Row) ->
    lists:all(fun(X)-> 1=<X andalso X=<9 end, Row).

%% how hard is the puzzle?

hard(M) ->
    lists:sum(
      [lists:sum(
	 [if is_list(X) ->
		  length(X);
	     true ->
		  0
	  end
	  || X <- Row])
       || Row <- M]).

%% choose a position {I,J,Guesses} to guess an element, with the
%% fewest possible choices

guess(M) ->
    Nine = lists:seq(1,9),
    {_,I,J,X} =
	lists:min([{length(X),I,J,X}
		   || {I,Row} <- lists:zip(Nine,M),
		      {J,X} <- lists:zip(Nine,Row),
		      is_list(X)]),
    {I,J,X}.

%% given a matrix, guess an element to form a list of possible
%% extended matrices, easiest problem first.
solve_guesses_pool([], _, _, _) -> [];
solve_guesses_pool([G|Gs], I, J, M) ->
    Status = pool_send_work(fun() -> solve_guesses_pool(Gs, I, J, M) end),
    [catch refine(update_element(M,I,J,G))] ++ pool_wait_for_value(Status).

guesses(M) ->
    {I,J,Guesses} = guess(M),
    Ms = solve_guesses_pool(Guesses, I, J, M),
    %%Ms = [catch refine(update_element(M,I,J,G)) || G <- Guesses],
    SortedGuesses =
	lists:sort(
	  [{hard(NewM),NewM}
	   || NewM <- Ms,
	      not is_exit(NewM)]),
    [G || {_,G} <- SortedGuesses].

update_element(M,I,J,G) ->
    update_nth(I,update_nth(J,G,lists:nth(I,M)),M).

update_nth(I,X,Xs) ->
    {Pre,[_|Post]} = lists:split(I-1,Xs),
    Pre++[X|Post].

%% prop_update() ->
%%     ?FORALL(L,list(int()),
%% 	    ?IMPLIES(L/=[],
%% 		     ?FORALL(I,choose(1,length(L)),
%% 			     update_nth(I,lists:nth(I,L),L) == L))).

%% solve a puzzle

solve(M) ->
    Solution = solve_refined(refine(fill(M))),
    case valid_solution(Solution) of
	true ->
	    Solution;
	false ->
	    exit({invalid_solution,Solution})
    end.

solve_refined(M) ->
    case solved(M) of
	true ->
	    M;
	false ->
        solve_one_pool(guesses(M))
	    %%solve_one(guesses(M))
    end.

solve_one([]) ->
    exit(no_solution);
solve_one([M]) ->
    solve_refined(M);
solve_one([M|Ms]) ->
    case catch solve_refined(M) of
	{'EXIT',no_solution} ->
	    solve_one(Ms);
	Solution ->
	    Solution
    end.

solve_one_pool([]) ->
    exit(no_solution);
solve_one_pool([M|Ms]) ->
    Status = pool_send_work(fun() -> catch solve_one_pool(Ms) end),
    case catch solve_refined(M) of
    {'EXIT',no_solution} ->
	    pool_wait_for_value(Status); %% It is not a must to actually look at the received messages
	Solution ->
        clear_msg_box(),
	    Solution
    end.
%% benchmarks
-define(EXECUTIONS,100).

bm(F) ->
    {T,_} = timer:tc(?MODULE,repeat,[F]),
    T/?EXECUTIONS/1000.

repeat(F) ->
    [F() || _ <- lists:seq(1,?EXECUTIONS)].

benchmarks(Puzzles) ->
    %%start_pool(erlang:system_info(schedulers) - 1),
    start_pool(6),
    Res = [{Name,bm(fun()->solve(M) end)} || {Name,M} <- Puzzles],
    pool ! {stop,self()},
    receive {pool,stopped} -> Res end.

benchmarks() ->
  {ok,Puzzles} = file:consult("problems.txt"),
  timer:tc(?MODULE,benchmarks,[Puzzles]).

%% check solutions for validity

valid_rows(M) ->
    lists:all(fun valid_row/1,M).

valid_row(Row) ->
    lists:usort(Row) == lists:seq(1,9).

valid_solution(M) ->
    valid_rows(M) andalso valid_rows(transpose(M)) andalso valid_rows(blocks(M)).


%% 1 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
benchmarks_par([]) -> [];
benchmarks_par([{Name, Puzzle}|Puzzles]) ->
    Thread = start_thread(fun() -> benchmarks_par(Puzzles) end),
    [{Name, bm(fun() -> solve(Puzzle) end)}] ++ wait_for_value(Thread).

benchmarks_par() ->
  {ok,Puzzles} = file:consult("problems.txt"),
  timer:tc(?MODULE,benchmarks_par,[Puzzles]).

%% 2 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% HELPERS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Regular thread starting---------------------------------------------------
start_thread(F) ->
    Parent = self(),
    Pid = spawn_link(fun() -> Parent ! {self(),F()} end),
    {spawned, Pid}.

wait_for_value(Pid) ->
    receive {Pid, Res} -> Res end.

%% Parallel mapping----------------------------------------------------------
chunk(N, XS) ->
    case N < length(XS) of
        true ->
            {LS, RS} = lists:split(N, XS),
            [LS] ++ chunk(N, RS);
        false ->
            [XS]
    end.

map_par_chunk(N, F, XS) ->
    Chunks = chunk(N, XS),
    map_par_chunk_inner(F, Chunks).

map_par_chunk_inner(_, []) -> [];
map_par_chunk_inner(F, [X|XS]) ->
    Thread = start_thread(fun() -> map_par_chunk_inner(F, XS) end),
    lists:map(F, X) ++ wait_for_value(Thread).

map_pool_chunk(N, F, XS) ->
    Chunks = chunk(N, XS),
    map_pool_chunk_inner(F, Chunks).

map_pool_chunk_inner(_, []) -> [];
map_pool_chunk_inner(F, [X|XS]) ->
    Status = pool_send_work(fun() -> map_pool_chunk_inner(F, XS) end),
    lists:map(F, X) ++ pool_wait_for_value(Status).

%% Pooling-------------------------------------------------------------------
start_pool(N) ->
    %% Register the spawned thread to the atom 'pool'. Spawn thread thad calls
    %% pool on N worker threads.
    true = register(
        pool, 
        spawn_link(
            fun() -> pool([worker() || _ <- lists:seq(1, N)]) end
        )
    ).

%% Entry function to the worker pool.
pool(Workers) ->
    pool(Workers, Workers).

%% The pool function waits idly for a message.
%% Avaliable is the available workers and All is all workers.
pool(Avaliable, All) ->
    receive 
    %% Try get a worker. Send the Pid of the process asking.
    {get_worker, Pid} ->
        case Avaliable of
            %% When no worker is avaliable, notify the sender
            %% process.
            [] ->
                Pid ! {pool, no_worker},
                pool(Avaliable, All); %% Continue 
            %% Otherwise, take a worker and send it back to the
            %% sender process.
            [W|Ws] ->
                Pid ! {pool, W},
                pool(Ws, All) %% Continue with one less avaliable worker.
        end;
    %% Take worker W, sent by a process, and continue with tat worker now
    %% avaliable.
    {return_worker, W} ->
        pool([W|Avaliable], All);
    %% Kill the pool. Requested by Pid.
    {stop, Pid} ->
        [unlink(W) || W <- All], %% Unlink all workers from pool so we can terminate them all before the pool is terminated.
        [exit(W, kill) || W <- All], %% Sent exit signal to all workers with reason 'kill'.
        unregister(pool), %% Stop associating the atom pool with this process.
        Pid ! {pool, stopped} %% Notify sender process and then terminate normally.
    end.

%% Spawn worker thread with function work.
worker() ->
    spawn_link(fun work/0).

%% Waits idly to recieve a task.
work() ->
    receive
    %% A task is defined by the sender process Pid, a unique reference and the work itself (a function).
    {task, Pid, Ref, F} ->
        Pid ! {Ref, F()}, %% Send the result of the work back to the sender process.
        catch pool ! {return_worker, self()}, %% Return to the pool. Error might occur because pool may not be registered? TODO: check this
        work() %% Continue wait for tasks.
    end.

%% Send work F to the pool
pool_send_work(F) ->
    %% Check if pool is created.
    case whereis(pool) of
        undefined ->
            ok; %% If not, return ok
        %% Get a worker from the pool otherwise.
        Pool -> Pool ! {get_worker, self()}
    end,
    receive
        %% Return work if no worker was avaliable.
        {pool, no_worker} ->
            {not_spawned, F};
        %% If worker was avaliable, send it work
        {pool, W} ->
            Ref = make_ref(),
            W ! {task, self(), Ref, F},
            {spawned, Ref} %% Return reference so result can be fetched
    end.

%% If pool could not provide worker, do work in this process.
pool_wait_for_value({not_spawned, F}) ->
    F();
%% Otherwise, wait for result.
pool_wait_for_value({spawned, R}) ->
    receive {R, X} -> X end.

clear_msg_box() ->
    receive
        _ -> clear_msg_box()
    after
        0 -> ok
    end.