%%% File    : yaws_tmpfile.erl
%%% Author  : Thomas Lindgren <thomasl@uploader-server.virtual.diino.net>
%%% Description : 
%%% Created :  2 Jun 2009 by Thomas Lindgren <thomasl@uploader-server.virtual.diino.net>

-module(yaws_tmpfile).
-export([name/1, open/2]).
-compile(export_all).

-include_lib("kernel/include/file.hrl").

%% This is inspired by the corresponding perl module:
%% - replace occurrences of "X" in the Stem string and opens the file
%%
%% Example:
%%   yaws_tmpfile:name("/tmp/yaws_XXXXXX")
%%     => "/tmp/yaws_O4DjAa"
%%   yaws_tmpfile:open("/tmp/yaws_XXXXXX", Opts) 
%%     => {ok, "/tmp/yaws_AaHGtt", FD}
%%
%% Note: open/2 is probably the one you want. Since we have no file locks, 
%% there is no way to actually securely reserve the filename though.
%% However, the window of danger is the time between the file:read_file_info/1
%% and the file:open/2, which is "short". The probability of a clash should
%% thus be "small".

-define(max_retry, 10).

name(Stem) ->
    %% check that directory exists first
    Dir = filename:dirname(Stem),
    case file:read_file_info(Dir) of
	{ok, Dir_info} when Dir_info#file_info.access == read_write
	                  ; Dir_info#file_info.access == write ->
	    name(Stem, ?max_retry);
	{ok, Dir_info} ->
	    exit({{?MODULE, name, 1}, directory_permissions});
	{error, Rsn} ->
	    exit({{?MODULE, name, 1}, directory_cannot_be_opened, Rsn})
    end.

name(Stem, N) when N > 0 ->
    File = random_name(Stem),
    case file:read_file_info(File) of
	{ok, _} ->
	    name(Stem, N-1);
	{error, _} ->
	    File
    end;
name(Stem, 0) ->
    exit({{?MODULE, name, 2}, too_many_collisions}).

open(Stem, Opts) ->
    File = name(Stem),
    case file:open(File, Opts) of
	{ok, FD} ->
	    {ok, File, FD};
	Err ->
	    Err
    end.
    
%% Replace each occurrence of "X" with a random char and keep the rest
%% unchanged.
%%
%% Alternative approach: count the number of X:s and generate N random
%% bytes at once.
%%
%% Note: we should optionally permit use of some other character than "X".

random_name("X" ++ Cs) ->
    [random_char()|random_name(Cs)];
random_name([C|Cs]) ->
    [C|random_name(Cs)];
random_name([]) ->
    [].

%% adjust this to size(index_char/1:s Avail_chars array)
-define(num_chars, 63).

random_char() ->
    index_char(random:uniform(?num_chars)-1).

%% Note: in recent erlang releases, the Avail_chars array should be
%% stored as a constant rather than being rebuilt at each call.

index_char(N) ->
    Avail_chars = <<"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		    "abcdefghijklmnopqrstuvwxyz"
		    "0123456789"
		    "_">>,
    <<_:N/binary, C, _/binary>> = Avail_chars,
    C.

%% Use this to seed the RNG. The old standby, erlang:now(),
%% is unsuitable.
%%
%% Note: seeding with 3*4 bytes may be excessive

seed() ->
    application:start(crypto),
    <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
    random:seed(A, B, C).


	    
