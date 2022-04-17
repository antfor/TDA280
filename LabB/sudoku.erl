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
    NewM =                                                                 %todo
	refine_rows(
	  transpose(
	    refine_rows(
	      transpose(
		unblocks(
		  refine_rows(
		    blocks(M))))))),
    if M==NewM ->
	    M;
       true ->
	    refine(NewM)
    end.

refine_rows(M) ->                                                          %todo
    lists:map(fun refine_row/1,M).

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

guesses(M) ->
    {I,J,Guesses} = guess(M),
    Ms = [catch refine(update_element(M,I,J,G)) || G <- Guesses],
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
	    solve_one(guesses(M))                                              %todo
    end.

solve_one([]) ->                                                           %todo
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

%% benchmarks

-define(EXECUTIONS,100).

bm(F) ->
    {T,_} = timer:tc(?MODULE,repeat,[F]),
    T/?EXECUTIONS/1000.

repeat(F) ->
    [F() || _ <- lists:seq(1,?EXECUTIONS)].

benchmarks(Puzzles) ->
    [{Name,bm(fun()->solve(M) end)} || {Name,M} <- Puzzles].

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

start_thread(F) ->
    Parent = self(),
    Pid = spawn_link(fun() -> Parent ! {self(),F()} end),
    {spawn,Pid}.

get_value({spawn,Pid}) ->
    receive {Pid,X} ->
	    X
    end.

parBench(Puzzles) ->
    F = fun(M) -> bm(fun()->solve(M) end) end,
    Pids = [{Name, start_thread(fun() -> F(M) end)} || {Name,M} <- Puzzles],
    [{Name, get_value(Pid)} || {Name, Pid} <- Pids].

benchmarkspar() ->
  {ok,Puzzles} = file:consult("problems.txt"),
  timer:tc(?MODULE,parBench,[Puzzles]).

%% 2 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


pmap(F,Xs) ->
    Threds = [ employ(fun() -> F(X) end) || X <- Xs ],
    [retire(Pid) || Pid <- Threds].

chunk(_,[]) -> [];
chunk(N, Xs) when N >= length(Xs) -> [Xs];
chunk(N, Xs) ->
    {As,Bs} = lists:split(N,Xs),
    [As | chunk(N,Bs)].

cmap(N,F,Xs) ->
    lists:concat(pmap ((fun(X) -> lists: map(F,X) end), chunk(N, Xs))).


pmap2(F,Xs) ->
    Threds = [ start_thread(fun() -> F(X) end) || X <- Xs ],
    [get_value(Pid) || Pid <- Threds].


cmap2(N,F,Xs) ->
    lists:concat(pmap2((fun(X) -> lists: map(F,X) end), chunk(N, Xs))).

prefine_rows(M) ->
    cmap(3,fun(R)-> catch refine_row(R) end,M).

prefine(0, M) -> refine(M);
prefine(N,M) ->
    NewM =                                                                 %todo
	prefine_rows(
	  transpose(
	    prefine_rows(
	      transpose(
		unblocks(
		  prefine_rows(
		    blocks(M))))))),
    if M==NewM ->
	    M;
       true ->
	   prefine(N-1, NewM)
    end.

prefine3(0, M) -> refine(M);
prefine3(N, M) ->

    P1 = employ(fun() -> (refine_rows(M)) end),
    P2 = employ(fun() -> (transpose(refine_rows(transpose(M)))) end),
%    P3 = employ(fun() -> (unblocks(refine_rows(blocks(M)))) end),

    M3 = unblocks(refine_rows(blocks(M))),

    M1 = retire(P1),
    M2 = retire(P2),
%    M3 = retire(P3),

    %Zip = lists:zip3(M1,M2,M3),
    %io:format("~p ~n~n",[length(Zip)]),
    %NewM = cmap(3,fun filter2/1,Zip),
    %NewM = lists:map(fun filter2/1,Zip),
    NewM =lists:zipwith3(fun filter/3,M1,M2,M3),

    if M==NewM ->
	    M;
       true ->
	    prefine3(N-1, NewM)
    end.


slength(N) when is_number(N)  -> 1;
slength(Xs) -> length(Xs).

filter(R1,R2,R3)->
    lists:zipwith3(fun member3/3, R1,R2,R3).

filter2({R1,R2,R3})->
    lists:zipwith3(fun member3/3, R1,R2,R3).

toList(N)  when is_number(N)  -> [N];
toList(Xs) -> Xs.

unList([N]) -> N;
unList(Xs)  -> Xs.

member(E1,E2,E3) ->
    Es = lists:map(fun(E) -> El = toList(E), {length(El),El} end, [E1,E2,E3]),
    [{_,Ea},{_,Eb},{_,Ec}] = lists:keysort(1, Es),
    unList(lists:filter(fun(E) -> lists:member(E,Eb) and lists:member(E,Ec) end,Ea)).


member2(E1,E2,E3) ->
    unList(lists:filter(fun(E) -> lists:member(E,toList(E2)) and lists:member(E,toList(E3)) end,toList(E1))).

member3(E1,E2,E3) ->
    unList((toList(E1) ++ toList(E2) ++ toList(E3)) -- [1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% pool solved

pool_solve(P) ->
    Workers = erlang:system_info(schedulers)-1,
%    io:format("~p ~n", [Workers]),
    start_pool(Workers),
    S = pool_solve1(P),
    pool ! {stop,self()},
    receive {pool,stopped} -> S end.

pool_solve1(M) ->
    Solution = solve_refined(prefine3(-1,fill(M))),
    case valid_solution(Solution) of
	true ->
	    Solution;
	false ->
	    exit({invalid_solution,Solution})
    end.

benchmarkspool(Puzzles) ->
    [{Name,bm(fun()->pool_solve(M) end)} || {Name,M} <- Puzzles].

benchmarkspool() ->
  {ok,Puzzles} = file:consult("problems.txt"),
  timer:tc(?MODULE,benchmarkspool,[Puzzles]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Worker pool from demo
%%

start_pool(N) ->
    true = register(pool,spawn_link(fun()->pool([worker() || _ <- lists:seq(1,N)]) end)).

pool(Workers) ->
    pool(Workers,Workers).

pool(Workers,All) ->
    receive
	{get_worker,Pid} ->
	    case Workers of
		[] ->
		    Pid ! {pool,no_worker},
		    pool(Workers,All);
		[W|Ws] ->
		    Pid ! {pool,W},
		    pool(Ws,All)
	    end;
	{return_worker,W} ->
	    pool([W|Workers],All);
	{stop,Pid} ->
	    [unlink(W) || W <- All],
	    [exit(W,kill) || W <- All],
	    unregister(pool),
	    Pid ! {pool,stopped}
    end.

worker() ->
    spawn_link(fun work/0).

work() ->
    receive
	{task,Pid,R,F} ->
	    Pid ! {R,F()},
	    catch pool ! {return_worker,self()},
	    work()
    end.

employ(F) ->
    case whereis(pool) of
	undefined ->
	    ok; %% we're stopping
	Pool -> Pool ! {get_worker,self()}
    end,
    receive
	{pool,no_worker} ->
	    {not_speculating,F};
	{pool,W} ->
	    R = make_ref(),
	    W ! {task,self(),R,F},
	    {speculating,R}
    end.

retire({not_speculating,F}) ->
    F();
retire({speculating,R}) ->
    receive
	{R,X} ->
	    X
    end.