%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This implements a page rank algorithm using map-reduce
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(page_rank).
-compile([export_all,nowarn_export_all]).

%% Use map_reduce to count word occurrences

map(Url,ok) ->
    [{Url,Body}] = dets:lookup(web,Url),
    Urls = crawl:find_urls(Url,Body),
    [{U,1} || U <- Urls].

reduce(Url,Ns) ->
    [{Url,lists:sum(Ns)}].

page_rank() ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_seq(fun map/2, fun reduce/2,
			      [{Url,ok} || Url <- Urls]).

page_rank_par() ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_par(fun map/2, 32, fun reduce/2, 32,
			      [{Url,ok} || Url <- Urls]).

%% 1 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
mapd(Url,ok) ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    [{Url,Body}] = dets:lookup(web,Url),
    Urls = crawl:find_urls(Url,Body),
    [{U,1} || U <- Urls].


page_rank_dis() ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_dis(fun mapd/2, 32, fun reduce/2, 32,
			      [{Url,ok} || Url <- Urls]).


%% 2 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

page_rank_pool() ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_pool(fun mapd/2, 32, fun reduce/2, 32,
			      [{Url,ok} || Url <- Urls]).

page_rank_pool2() ->
  {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
  Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
  map_reduce:map_reduce_pool2(fun mapd/2, 32, fun reduce/2, 32,
			      [{Url,ok} || Url <- Urls]).
%% 3 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% B %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
page_rank_seq_ok() ->
    page_rank_dis(),
    ok.
page_rank_par_ok() ->
    page_rank_par(),
    ok.
page_rank_dis_ok() ->
    page_rank_dis(),
    ok.

page_rank_pool_ok() ->
    page_rank_pool(),
    ok.

page_rank_pool2_ok() ->
    page_rank_pool2(),
    ok.

benchmark_seq() ->
  timer:tc(?MODULE, page_rank_ok , []).

benchmark_par() ->
  timer:tc(?MODULE, page_rank_par_ok, []).

benchmark_dis() ->
  timer:tc(?MODULE, page_rank_dis_ok, []).

benchmark_pool() ->
  timer:tc(?MODULE, page_rank_pool_ok, []).

benchmark_pool2() ->
  timer:tc(?MODULE, page_rank_pool2_ok, []).
