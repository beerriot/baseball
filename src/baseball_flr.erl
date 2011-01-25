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
%%
%%      This module performs the same computation as the baseball
%%      module, but does so by storing fixed-length records ("flr" in
%%      the module name), instead of re-parsing the line-based, CSV
%%      format provided by Retrosheet.  The conversion to fixed-length
%%      records is done at load-time, so data loaded with
%%      baseball:load_events/1 is incompatible with
%%      baseball_flr:batting_average/2 (and data loaded wth
%%      baseball_flr:load_events/1 is incompatible with
%%      baseball:batting_average/2).
-module(baseball_flr).

-export([
         load_events/1,
         batting_average/2
        ]).

-export([
         ba_map/3,
         count_at_bats/2,
         ba_reduce/2
        ]).

-define(FIELD_SIZE_PLAYERID, 8).
-define(FIELD_SIZE_EVENT, 50).
-define(RECORD_SIZE, (?FIELD_SIZE_PLAYERID+?FIELD_SIZE_EVENT)).
-define(RECORDS_PER_BLOCK, 17000). %% 58*17k =~ 986k blocks

%% @spec load_events(string()) -> ok
%% @doc Loads all of the "play" records from event files (*.EV*) into
%%      a luwak file named for the directory plus a "_flr" suffix,
%%      that is, it concatenates all events files into one big Luwak file.
load_events(Directory) ->
    true = filelib:is_dir(Directory),
    Name = iolist_to_binary([filename:basename(Directory), <<"_flr">>]),
    {ok, Client} = riak:local_client(),
    {ok, LuwakFile} = luwak_file:create(
                        Client, Name,
                        [{block_size, ?RECORD_SIZE*?RECORDS_PER_BLOCK}],
                        dict:new()),
    LuwakStream = luwak_put_stream:start_link(Client, LuwakFile, 0, 5000),
    filelib:fold_files(Directory,
                       ".*\.EV.",  %% only events files
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
    {match, Plays} = re:run(FileData,
                              <<"^play,.,.,(.+),.*,.*,(.*)$">>, %">>,
                              [global, multiline, {newline, crlf},
                               {capture, all_but_first, binary}]),
    Formatted = [ format_play(PlayerId, Events)
                  || [PlayerId, Events] <- Plays ],
    luwak_put_stream:send(LuwakStream, Formatted),
    LuwakStream.

%% @spec format_play(binary(), binary()) -> iolist()
%% @doc Arrange the details of a play record in fixed-length format.
%%      The record is simply the 8-byte player id, followed by the
%%      contents of the events field, followed by enough extra zeros
%%      to fill the 10-byte field.
format_play(PlayerId, Events) ->
    %% bomb if player id is not expected size
    ?FIELD_SIZE_PLAYERID = size(PlayerId),
    Padding = case ?FIELD_SIZE_EVENT-size(Events) of
                  0   -> <<>>;
                  Pad -> list_to_binary(lists:duplicate(Pad, 0))
                         %% this will bomb if size(Events) > ?FIELD_SIZE
                         %% (purposefully - keeps from writing bad data)
              end,
    [PlayerId, Events, Padding].

%% @spec batting_average(binary(), binary()) -> integer()
%% @doc Find the batting average for the specified player in the
%%      specified Luwak file.
%%      The basic idea is to evaluate a map on the file's blocks that
%%      picks out every at-bat for the player id, and then reduce the
%%      results.
batting_average(Filename, PlayerId) ->
    {ok, Client} = riak:local_client(),
    {ok, [{Hits,AtBats}]} =
        Client:mapred(
          {modfun, luwak_mr, file, Filename},
          [{map, {modfun, baseball_flr, ba_map}, PlayerId, false},
           {reduce, {modfun, baseball_flr, ba_reduce}, none, true}]),
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
    count_at_bats_rec(Data, PlayerId, 0, 0).

count_at_bats_rec(<<PlayerId:?FIELD_SIZE_PLAYERID/binary,
                    Event:?FIELD_SIZE_EVENT/binary,
                    Rest/binary>>,
                  PlayerId, Hits, AtBats) ->
    count_at_bats_rec(Rest, PlayerId,
                      case is_hit(Event) of
                          true  -> Hits+1;
                          false -> Hits
                      end,
                      case is_at_bat(Event) of
                          true  -> AtBats+1;
                          false -> AtBats
                      end);
count_at_bats_rec(<<_:?RECORD_SIZE/binary, Rest/binary>>,
                  PlayerId, Hits, AtBats) ->
    count_at_bats_rec(Rest, PlayerId, Hits, AtBats);
count_at_bats_rec(<<>>, _PlayerId, Hits, AtBats) ->
    {Hits, AtBats}.

is_hit(Event) ->
    match == re:run(Event,
                    "^("
                    "S[0-9]" % single
                    "|D[0-9]" % double
                    "|T[0-9]" % triple
                    "|H[^P]" % home run
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
