%% -------------------------------------------------------------------
%% baseball: utilities for computing baseball stats on Riak
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%  http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Compute baseball stats using Riak Map/Reduce.
%%
%%      This module works with data from the Retrosheet archive
%%      (http://www.retrosheet.org/) to compute baseball statistics
%%      (currently only batting average).
-module(baseball).

-export([
         load_events/1,
         batting_average/2
        ]).

-export([
         ba_map/3,
         count_at_bats/2,
         find_uncounted_chunks/3,
         ba_reduce/2
        ]).

%% @type map_result() = bat_count()|chunk()
%% @type bat_count() = {Hits::integer(), AtBats::integer()}
%% @type chunk() = {chunk_type(), Offset::integer(), Data::binary()}
%% @type chunk_type() = suffix|prefix

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
    {ok, Results} =
        Client:mapred(
          {modfun, luwak_mr, file, Filename},
          [{map, {modfun, baseball, ba_map}, PlayerID, false},
           {reduce, {modfun, baseball, ba_reduce}, PlayerID, true}]),
    %% filter out left over, unused segments
    %% (the first segment will generate an unused suffix, unless it's
    %% a whole play record for the given PlayerID; similarly for the
    %% final segment generating an unused prefix)
    [{Hits,AtBats}] = [ {H,AB} || {H,AB} <- Results ],
    trunc(1000*Hits/AtBats).

%% @spec ba_map(luwak_block(), term(), binary()) -> [map_result()]
%% @doc Find the at-bat results in this block for the given player.
%%      Result is a list of at least one element, a tuple of the form
%%      {Hits, AtBats}.  The result may also include one suffix and/or
%%      one prefix record.
ba_map(LuwakBlock, Offset, PlayerId) ->
    Data = luwak_block:data(LuwakBlock),
    [count_at_bats(Data, PlayerId)
     |find_uncounted_chunks(Data, PlayerId, Offset)].

%% @spec ba_reduce([map_result()], binary()) -> [map_result()]
%% @doc Sum the intermediate results produced by the map phase.
%%      Result is a list of of at least one element, a tuple of the
%%      form {Hits, AtBats}.  The result may also include suffix and
%%      prefix records that could not be matched to other prefix and
%%      suffix records.
ba_reduce(CountsAndChunks, PlayerId) ->
    {Chunks, Counts} = resolve_chunks(CountsAndChunks, PlayerId),
    {HitList, AtBatList} = lists:unzip(Counts),
    [{lists:sum(HitList), lists:sum(AtBatList)}|Chunks].

%% @spec count_at_bats(binary(), binary()) -> bat_count()
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

%% @spec count_at_bats_fold([binary()], bat_count()) -> bat_count()
count_at_bats_fold([Event], {Hits, AtBats}) ->
    {case is_hit(Event) of
         true  -> Hits+1;
         false -> Hits
     end,
     case is_at_bat(Event) of
         true -> AtBats+1;
         false -> AtBats
     end}.

%% @spec is_hit(binary()) -> boolean()
is_hit(Event) ->
    match == re:run(Event,
                    "^("
                    "S[0-9]" % single
                    "|D[0-9]" % double
                    "|T[0-9]" % triple
                    "|H([^P]|$)" % home run
                    ")",
                    [{capture, none}]).

%% @spec is_at_bat(binary()) -> boolean()
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

%% @spec find_uncounted_chunks(binary(), binary(), integer()) -> [chunk()]
%% @doc Finds chunks that are not complete records at the starts and
%%      ends of blocks.  Return is an empty list if there are no
%%      incomplete records, a list of two elements if there is an
%%      incomplete record at the start and at the end, or a list of
%%      one element if there is only a incomplete record at either the
%%      start or the end.
%%
%%      Incomplete records at the start of a block are marked
%%      'suffix', and their offset is that of the first byte of their
%%      data.  Incomplete records at the end of a block are marked
%%      'prefix', and their offset is that of the byte immediately
%%      following their last byte.  The intention is that a prefix
%%      should be matched with its suffix by matching the offset of
%%      the two.
%%
%%      ------------blockN---------|---------blockN+1------------
%%      --|---recordR---|--prefix--|--suffix--|---recordR+2---|--
%%                                  ^
%%                         offset field of both
find_uncounted_chunks(Data, PlayerId, Offset) ->
    Suffix = re:run(Data, "^([^\r]*)", [{capture, all_but_first, binary},
                                        {newline, crlf}]),
    Prefix = re:run(Data, "\n([^\n]*)$", [{capture, all_but_first, binary},
                                          {newline, crlf}, dollar_endonly]),
    %% count_at_bats is called to make sure that this thing that
    %% looks like a chunk hasn't already affected the count
    [ {suffix, Offset, S} || {match, [S]} <- [Suffix],
                             {0,0} == count_at_bats(S, PlayerId) ]
    ++
    [ {prefix, Offset+size(Data), P} || {match, [P]} <- [Prefix],
                                        {0,0} == count_at_bats(P, PlayerId)].

%% @spec resolve_chunks(map_result(), binary()) -> {[chunk()], [bat_count()]}
%% @doc Given a list of {Hit, AtBat} counts and prefix-suffix chunks,
%%      transform as many prefix-suffix chunks into additional {Hit,
%%      AtBat} counts as possible, and return two lists, the first
%%      being the remaining un-transformed chunks, and the second
%%      being all {Hit, AtBat} counts now available.
resolve_chunks(CountsAndChunks, PlayerId) ->
    {Chunks, HABs} = lists:partition(fun(T) -> size(T) == 3 end,
                                     CountsAndChunks),
    {Remaining, NewRecords} = match_chunks(Chunks),
    NewHABs = [ count_at_bats(R, PlayerId) || R <- NewRecords ],
    {Remaining, NewHABs++HABs}.

%% @spec match_chunks([chunk()]) -> {[chunk()], [binary()]}
match_chunks(Chunks) ->
    {Prefixes, Suffixes} = lists:partition(fun({T,_,_}) -> T == prefix end,
                                           Chunks),
    lists:foldl(fun match_chunks_fold/2, {Suffixes, []}, Prefixes).

%% @spec match_chunks_fold(chunk(), {[chunk()], [binary()]})
%%          -> {[chunk()], [binary()]}
match_chunks_fold(P={prefix, Offset, PreData}, {Remaining, Resolved}) ->
    case lists:keytake(Offset, 2, Remaining) of
        {value, {suffix, Offset, SufData}, NewRemaining} ->
            {NewRemaining, [iolist_to_binary([PreData,SufData])|Resolved]};
        _ ->
            {[P|Remaining], Resolved}
    end.
