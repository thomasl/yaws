%%% File    : yaws_tmpfile.erl
%%% Author  : Thomas Lindgren <thomasl@uploader-server.virtual.diino.net>
%%% Description : 
%%% Created :  2 Jun 2009 by Thomas Lindgren <thomasl@uploader-server.virtual.diino.net>

-module(yaws_tmpfile).
-export([name/1, open/1, open/2]).
-export([check_exists/1]).
-compile(export_all).

-include_lib("kernel/include/file.hrl").

-undef(info).
-undef(error).
%% UNFINISHED - should be wrapped with checking, etc
-define(info(Str, Xs), logging:info(?MODULE, ?LINE, Str, Xs)).
-define(error(Str, Xs), logging:error(?MODULE, ?LINE, Str, Xs)).

%% This is inspired by the File::Temp perl module
%% - replace occurrences of "X" in the Stem string and opens the file
%%
%% Example:
%%   yaws_tmpfile:name("/tmp/yaws_XXXXXX")
%%     => "/tmp/yaws_O4DjAa"
%%   yaws_tmpfile:open("/tmp/yaws_XXXXXX", Opts) 
%%     => {ok, "/tmp/yaws_AaHGtt", FD}
%% or even
%%   yaws_tmpfile:open(Actual_file ++ "_XXXXXX", Opts)
%%     => {ok, "/path/to/file_Aa04Eh", FD}
%%
%% Note: open/2 is probably the one you want. Since we have no file locks, 
%% there is no way to actually securely reserve the filename though.
%% However, the window of danger is the time between the file:read_file_info/1
%% and the file:open/2, which is "short". The probability of a clash should
%% thus be "small".

-define(max_retry, 10).
-define(suffix_chars, 6).

name(Stem) ->
    name(Stem, ?suffix_chars, ?max_retry);
name(Stem) ->
    %% OBSOLETE (for now)
    %% check that directory exists first
    Dir = filename:dirname(Stem),
    case file:read_file_info(Dir) of
	{ok, Dir_info} when Dir_info#file_info.access == read_write
	                  ; Dir_info#file_info.access == write ->
	    name(Stem, ?suffix_chars, ?max_retry);
	{ok, _Dir_info} ->
	    exit({{?MODULE, name, 1}, directory_permissions, Dir});
	{error, Rsn} ->
	    Err_str =
		case (catch check_exists(Dir)) of
		    {'EXIT', Rsn2} ->
			%% error in the error analyzer function ...
			io_lib:format("Error while checking ~s: ~p "
				      "[original error: ~p]\n",
				      [Dir, Rsn2]);
		    Err ->
			format_error(Err)
		end,
	    ?error("Directory ~s not found: ~s\n", [Dir, Err_str]),
	    exit({{?MODULE, name, 1}, directory_cannot_be_opened, Dir, Rsn})
    end.

%% Generate a random suffix, ensure that generated filename does not
%% exist.

name(Stem, SuffixChars, N) when N > 0 ->
    File = random_name(Stem, SuffixChars),
    case file:read_file_info(File) of
	{ok, _} ->
	    name(Stem, SuffixChars, N-1);
	{error, _} ->
	    File
    end;
name(Stem, _SuffixChars, 0) ->
    exit({{?MODULE, name, 2}, too_many_collisions, Stem}).

open(Stem) ->
    open(Stem, [write]).

%% Open the tmpfile
%% - if the parent dir does not exist, create it
%%
%% UNFINISHED
%% - not webdav compliant ...
%% - could be unsafe for general settings, but OK for our backup client

open(Stem, Opts) ->
    File = name(Stem),
    case file:open(File, Opts) of
	{ok, FD} ->
	    {ok, File, FD};
	{error, enoent} ->
	    ?error("Error ENOENT when trying to open ~p ... "
		   "try to create parent dir\n",
		   [File]),
	    %% Dir = filename:dirname(File),
	    case filelib:ensure_dir(File) of
		ok ->
		    case file:open(File, Opts) of
			{ok, FD} ->
			    {ok, File, FD};
			Still_Err ->
			    ?error("Still unable to create file ~p "
				   "(error ~p), give up\n",
				   [File, Still_Err]),
			    Still_Err
		    end;
		EnsureErr ->
		    ?error("Error ~p when trying to create parent to ~p,"
			   " give up\n",
			   [EnsureErr, File]),
		    EnsureErr
	    end;
	Err ->
	    ?error("Error ~p when trying to open ~p, give up\n",
		   [Err, File]),
	    Err
    end.
    
%% Replace each occurrence of "X" with a random char and keep the rest
%% unchanged.
%%
%% Alternative approach: count the number of X:s and generate N random
%% bytes at once.
%%
%% Note: we should optionally permit use of some other character than "X".

random_name(File, SuffixChars) ->
    File ++ "_" ++ random_suffix(SuffixChars).

random_name_old("X" ++ Cs) ->
    [random_char()|random_name_old(Cs)];
random_name_old([C|Cs]) ->
    [C|random_name_old(Cs)];
random_name_old([]) ->
    [].

random_suffix(0) ->
    "";
random_suffix(N) when N > 0 ->
    [random_char()|random_suffix(N-1)].

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

%% Check that the components of a file system path exist.
%% Return the leftmost path that does not exist, or "" if it's okay 

check_path_exists(FsPath) ->
    Segs = string:tokens(FsPath, "/"),
    check_path_segs(Segs).

check_path_segs(Segs) ->
    check_path_segs(Segs, []).

check_path_segs([Seg|Segs], Curr) ->
    Next = filename:join(Curr, Seg),
    case location_exists(Next) of
	false ->
	    %% this is the leftmost non-existent path
	    Next;
	true ->
	    check_path_segs(Segs, Next)
    end.

-include_lib("kernel/include/file.hrl").

location_exists(Path) ->
    case file:read_file_info(Path) of
	{error, _Rsn} ->
	    false;
	{ok, _Info} ->
	    true
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% The following are for testing retry-based operation.
%%

%% Create a temporary file name. If this fails due to directory not existing,
%% retry for a while ... Why do this?
%%  1. directory created by dav server
%%  2. file written by upload server (this one)
%%
%% Step 2 apparently can occur before step 1 has been propagated through
%% the system. (That is, the directory is not visible yet to the upload
%% server.)

retry_name(Stem) ->
    retry_name(Stem, initial_sleep()).

retry_name(Stem, Sleep) ->
    MaxSleep = max_sleep(),
    if
	Sleep > MaxSleep ->
	    name(Stem);
	true ->
	    case catch name(Stem) of
		{'EXIT', {_MFA, directory_cannot_be_opened, _Dir, _Rsn}} ->
		    %% ?error("Unable to open ~p: ~p: retry", [Dir, Rsn])
		    sleep(Sleep),
		    retry_name(Stem, extend_sleep(Sleep));
		{'EXIT', Rsn} ->
		    exit(Rsn);
		Res ->
		    Res
	    end
    end.

initial_sleep() ->
    1000.

max_sleep() ->
    10000.

sleep(N) ->
    io:format("Retry: Sleep ~p\n", [N]),
    receive
	after N ->
		ok
	end.

extend_sleep(N) ->
    2*N.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Split the path into tokens, check that each segment exists
%% - this could be optimized, e.g., we could check it from leaf to root instead

%% NB: this will return an error if the indicated file does not exist;
%% if it's about to be created, that's actually okay

check_exists(File) ->
    Segs = string:tokens(File, "/"),
    check_exists(Segs, []).

check_exists([], Path) ->
    {ok, Path};
check_exists([Seg|Segs], PrevPath) ->
    Path = filename:join(PrevPath, Seg),
    case file:read_file_info(Path) of
	{ok, #file_info{type=T}} ->
	    case T of
		directory ->
		    check_exists(Segs, Path);
		regular ->
		    case Segs of
			[] ->
			    {ok, Path};
			_ ->
			    %% regular file but we expected it to be dir
			    {error, 
			     premature_regular_file, 
			     {path, Path, {remains, Segs}}}
		    end;
		Other ->
		    {error, 
		     {bad_file_type, Other},
		     {path, Path, {remains, Segs}}}
	    end;
	{error, Err} ->
	    {error, Err, {path, Path, {remains, Segs}}}
    end.

format_error({ok, Path}) ->
    io_lib:format("No error: ~s exists", [Path]);
format_error({error, Err, {path, P, {remains, Segs}}}) ->
    io_lib:format("Error ~p for path ~s (followed by: ~s)\n", 
		  [Err, P, string:join(Segs, "/")]).
