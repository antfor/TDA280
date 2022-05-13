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

map_dist(Url, ok)  ->
    {ok, web} = dets:open_file(web, [{file, "web.dat"}]),
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
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_par(fun map/2, 32, fun reduce/2, 32, 
			      [{Url,ok} || Url <- Urls]).

page_rank_dist() ->
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_dist(fun map_dist/2, 32, fun reduce/2, 32, 
			      [{Url,ok} || Url <- Urls]).

page_rank_dist_pool() ->
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_dist_pool(fun map_dist/2, 32, fun reduce/2, 32, 
			      [{Url,ok} || Url <- Urls]).

page_rank_dist_pool_f() ->
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_dist_pool_f(fun map_dist/2, 32, fun reduce/2, 32, 
			      [{Url,ok} || Url <- Urls]).

page_rank_dist_pool_ok() ->
    page_rank_dist_pool(),
    ok.

page_rank_dist_pool_bench() ->
  timer:tc(?MODULE, page_rank_dist_pool_ok, []).