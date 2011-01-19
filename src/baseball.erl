-module(baseball).

-export([
         load_events/1,
         batting_average/2
        ]).

-export([
         ba_map/3,
         count_at_bats/2,
         ba_reduce/2
        ]).

%% @spec load_events(string()) -> ok
%% @doc Loads all of the event files (*.EV*) into a luwak file named
%%      for the directory, that is, it concatenates all events files
%%      into one big Luwak file.
load_events(Directory) ->
    true = filelib:is_dir(Directory),
    Name = iolist_to_binary(filename:basename(Directory)),
    {ok, Client} = riak:local_client(),
    {ok, LuwakFile} = luwak_file:create(Client, Name, dict:new()),
    LuwakStream = luwak_put_stream:start_link(Client, LuwakFile, 0, 5000),
    filelib:fold_files(Directory,
                       ".*\.EV?",  %% only events files
                       false,      %% non-recursive
                       fun load_events_fold/2,
                       LuwakStream),
    luwak_put_stream:close(LuwakStream),
    ok.

%% @spec load_events_fold(binary(), luwak_stream()) -> luwak_stream()
%% @doc Fold function for load_events.  Just throws the file data
%%      directly into the stream.
load_events_fold(File, LuwakStream) ->
    {ok, FileData} = file:read_file(File),
    luwak_put_stream:send(LuwakStream, FileData),
    LuwakStream.

%% @spec batting_average(binary(), binary()) -> integer()
%% @doc Find the batting average for the specified player in the
%%      specified Luwak file.
%%      The basic idea is to evaluate a map on the file's blocks that
%%      picks out every at-bat for the player id, and then reduce the
%%      results.
batting_average(Filename, PlayerID) ->
    {ok, Client} = riak:local_client(),
    {ok, [{Hits,AtBats}]} =
        Client:mapred({modfun, luwak_mr, file, Filename},
                      [{map, {modfun, baseball, ba_map}, PlayerID, false},
                       {reduce, {modfun, baseball, ba_reduce}, none, true}]),
    trunc(1000*Hits/AtBats).

%% @spec ba_map(luwak_block(), term(), binary()) -> [{integer(), integer()}]
%% @doc Find the at-bat results in this block for the given player.
%%      Result is a one-element list of {Hits, AtBats}
ba_map(LuwakBlock, _, PlayerId) ->
    Data = luwak_block:data(LuwakBlock),
    [count_at_bats(Data, PlayerId)].

%% @spec ba_reduce([{integer(), integer()}], binary())
%%          -> [{integer(), integer()}]
%% @doc Sum the intermediate results produced by the map phase
%%      Result is a one-element lis of {Hits, AtBats}.
ba_reduce(Counts, _) ->
    {HitList, AtBatList} = lists:unzip(Counts),
    [{lists:sum(HitList), lists:sum(AtBatList)}].

%% @spec count_at_bats(binary(), binary()) -> {integer(), integer()}
%% @doc Find the at-bat results in this block for the given player.
%%      Result is {Hits, AtBats}
count_at_bats(Data, PlayerId) ->
    Re = [<<"^play,.,.,">>,PlayerId,<<",.*,.*,(.*)$">>], %">>],
    case re:run(Data, iolist_to_binary(Re),
                [{capture, all_but_first, binary},
                 global, multiline, {newline, crlf}]) of
        {match, Plays} ->
            lists:foldl(fun count_at_bats_fold/2, {0,0}, Plays);
        nomatch ->
            {0, 0}
    end.

count_at_bats_fold([Event], {Hits, AtBats}) ->
    {case is_hit(Event) of
         true  -> Hits+1;
         false -> Hits
     end,
     case is_at_bat(Event) of
         true -> AtBats+1;
         false -> AtBats
     end}.

is_hit(Event) ->
    match == re:run(Event,
                    "^("
                    "S[0-9]" % single
                    "|D[0-9]" % double
                    "|T[0-9]" % triple
                    "|H([^P]|$)" % home run
                    ")",
                    [{capture, none}]).

is_at_bat(Event) ->
    nomatch == re:run(Event,
                      "^("
                      "NP" % no-play
                      "|BK" % balk
                      "|CS" % caught stealing
                      "|DI" % defensive interference
                      "|OA" % base runner advance
                      "|PB" % passed ball
                      "|WP" % wild pitch
                      "|PO" % picked off
                      "|SB" % stole base
                      "|I?W" % walk
                      "|HP" % hit by pitch
                      "|SH" % sacrifice (but)
                      ")",
                      [{capture, none}]).
