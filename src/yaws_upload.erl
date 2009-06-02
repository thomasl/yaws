%%% File    : yaws_upload.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created :  9 May 2009 by Thomas Lindgren <>
%%
%% Optimized PUT, invoked via yaws_server.
%%
%% The idea here is to handle PUTs memory efficiently, by reading carefully
%% and writing the response to the destination file. Directories need not
%% be handled (use MKCOL).
%% Should handle direct and chunked transfers and return the appropriate
%% response codes.
%%  Byte ranges (Content-Range) not handled initially.
%%
%% RFC 2616 (9.6)
%% - store supplied entity under Request-URI
%%   * resource is new: 201 (Created)
%%   * resource is modified: 200 (OK) or 204 (No Content)
%%   * unable to modify: appropriate response
%%   * server wants to apply to other URI: 301 (Moved Permanently)
%%   * MUST NOT ignore Content-* (e.g., Content-Range), return 501 (Not Implemented)
%%     if unable to handle
%% - (8.2.3): use of 100-Continue
%%   * client must send "Expect: 100-continue" header if use
%%     must NOT send it for requests w/o response body
%%   * server: on receiving 100-continue, respond with "100 (Continue)" status
%%     and continue reading, OR respond with a final status code
%%     - server must eventually send a final status code anyway
%%     - final status code may include 417 (Expectation Failed)
%%     - "mostly" for HTTP/1.1, see spec for HTTP/1.0
%%   * proxy: not handled, see spec
%%
%% RFC 4918 (9.7)
%% - 9.7.1 if no appropriately scoped parent: 409 (Conflict)
%% - properties may be recomputed as part of PUT
%% - content-type SHOULD be provided by client, if not, server may choose
%%   one or none
%% - 9.7.2 PUT to existing collection MAY result in error 405 (Method not allowed)
%%
%% UNFINISHED
%% - uses #sc.partial_post_size to control max upload read
%% - byte ranges not yet handled
%% - webdav locks not yet handled
%% - SSL handling uncertain

-module(yaws_upload).
-export([body/3]).

-compile(export_all).

-include("../include/yaws_dav.hrl").
-include("../include/yaws_api.hrl").
-include("../include/yaws.hrl").
-include("yaws_debug.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/file.hrl").


-define(elog(X,Y), error_logger:info_msg("*elog ~p:~p: " X,
                                         [?MODULE, ?LINE | Y])).

%% UNFINISHED
%% - should map to a proper logger in time
%% - something else uses dbg/2, but not useful
-undef(dbg).
-define(dbg(Str, Xs), io:format("~p:~p: " Str, [?MODULE, ?LINE |Xs])).

%% UNFINISHED
%% - default settable some better way?
-define(read_segment_len, 8192).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% The body/3 method is invoked by yaws_server to complete an upload
%% (or reject it).
%%
%% UNFINISHED
%% - check that Content-Range header is not present
%%   * or handle it, file:pwrite/2
%% - URI encoding/decoding? esp. combined with UTF-8 ...
%% - map URL to storage with external module (config'd per server)
%% - upload+move rather than overwrite existing file
%% - measure size of process when done

body(CliSock, Req, Head) ->
    ?dbg("... body_method ...\n", []),
    SC = get(sc),
    SSL = yaws:is_ssl(SC),
    ok = yaws:setopts(CliSock, [{packet, raw}, binary], SSL),
    PPS = SC#sconf.partial_post_size,
    ?dbg("... content-length = ~p, te = ~p\n", 
	      [Head#headers.content_length, 
	       Head#headers.transfer_encoding]),
    ?dbg("Headers: ~p\n", [Head]),
    %% open tempfile as FD
    %% if upload fails, delete tempfile
    %% when upload completes, move tempfile to destfile 
    DocRoot = SC#sconf.docroot,
    File = file_of_url(DocRoot, Req),
    PrevExist = dest_file_exists(File),
    {ok, TmpFile, FD} = yaws_tmpfile:open(File, [append, raw, delayed_write]),
    ?dbg("upload ~p -> tmpfile ~p\n", [Req#http_request.path, File]),
    %% Note: 
    try 
	case respond_to_100(CliSock, Head) of
	    ok ->
		case Head#headers.content_length of
		    undefined ->
			case Head#headers.transfer_encoding of
			    "chunked" ->
				%% transfer in chunks	
				?dbg("chunked transfer\n", []),
				SegLen = upload_segment_length(PPS),
				upload_chunks(CliSock, FD, SegLen, SSL),
				?dbg("rename ~p -> ~p\n", [TmpFile, File]),
				ok = file:rename(TmpFile, File),
				case PrevExist of
				    true ->
					deliver_204(CliSock, Req);
				    false ->
					deliver_201(CliSock, Req)
				end;
			    Enc ->
				?dbg("Unknown transfer-encoding: ~p\n", 
				     [Enc]),
				deliver_500(CliSock, Req)
			end;
		    Len_lst ->
			Len = list_to_integer(Len_lst),
			?dbg("content-length: ~p\n", [Len]),
			SegLen = upload_segment_length(PPS),
			upload_body_length(
			  CliSock, FD, Len, SegLen, SSL
			 ),
			?dbg("rename ~p -> ~p\n", [TmpFile, File]),
			ok = file:rename(TmpFile, File),
			case PrevExist of
			    true ->
				deliver_204(CliSock, Req);
			    false ->
				deliver_201(CliSock, Req)
			end
		end;
	    Err ->
		%% final response has been sent, done
		?dbg("error respond_to_100: ~p\n", [Err]),
		deliver_500(CliSock, Req)
	end
    catch
	Type:Exn ->
	    ?dbg("Exception ~p:~p when uploading ~p -> ~p\n",
		 [Type, Exn, Req#http_request.path, TmpFile])
    after
	%% always close and delete the tempfile when done
	file:close(FD),
        case file:delete(TmpFile) of
	    ok ->
		?dbg("Upload failed, deleted tmpfile ~p\n", [TmpFile]);
	    _ ->
		%% delete fails if file has been renamed, which means
		%% upload okay
		ok
	end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% "Created"

deliver_201(CliSock, Req) ->
    yaws_server:deliver_xxx(CliSock, Req, 201).
    
%% "Updated"

deliver_204(CliSock, Req) ->
    yaws_server:deliver_xxx(CliSock, Req, 204).
    
%% "Internal Server Error"

deliver_500(CliSock, Req) ->
    yaws_server:deliver_xxx(CliSock, Req, 500).

%% "Not Implemented"

deliver_501(CliSock, Req) ->
    yaws_server:deliver_xxx(CliSock, Req, 501).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% The segment length is the size of each chunk read from the socket
%% (not to be confused with chunked encoding). This is defined by the
%% partial_post_size parameter if available, otherwise set to a "sensible
%% default".

upload_segment_length(nolimit) ->
    ?read_segment_len;
upload_segment_length(N) when integer(N), N > 0 ->
    N;
upload_segment_length(X) ->
    exit({unknown_post_size_value, X}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% UNFINISHED
%% - resolve to actual fs path => use rewrite mod?
%% - e.g., use docroot ++ path for abspath
%%   or a "map to storage handler"
%% - handling of UTF-8 in filenames?

%%file_of_url(DocRoot, Req) ->
%%    %% for testing
%%    "/dev/null";
file_of_url(DocRoot, Req) ->
    case Req#http_request.path of
	{abs_path, Path} ->
	    Tempfile = DocRoot ++ Path,
	    ?dbg("Path = ~p, temp = ~p\n", [Path, Tempfile]),
	    Tempfile;
	Err ->
	    ?dbg("UNFINISHED - Unable to map ~p\n", [Err])
    end.

dest_file_exists(File) ->
    case file:read_file_info(File) of
	{ok, _Finfo} ->
	    %% check permissions? e.g., write perm
	    %% for a web server, you generally own all the data
	    %% so probably not needed
	    true;
	{error, enoent} ->
	    false;
	{error, Other} ->
	    ?dbg("File check for ~s: error ~p\n", [File, Other])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% All uploads are run as loops, reading at most a limited number of
%% bytes before shuttling them to the output fd.
%%
%% There are a number of variations on this theme:
%% - body length known
%% - body length unknown
%% - body uses chunked transfer-encoding
%%
%% UNFINISHED
%% - we should use yaws:do_recv/3 instead of gen_tcp:recv/3, but
%%   would like to have a read timeout too

upload_body_length(CliSock, FD, Len, SegLen, SSL) ->
    yaws:setopts(CliSock, [binary, {packet, raw}], SSL),
    upload_loop(CliSock, FD, Len, SegLen, SSL).

upload_loop(CliSock, FD, Len, SegLen, SSL) when Len > 0 ->
    ReadLen = min(Len, SegLen),
    ?dbg("wait for ~p bytes ...\n", [ReadLen]),
    %% case gen_tcp:recv(CliSock, ReadLen, ?upload_read_timeout) of
    case yaws:do_recv(CliSock, ReadLen, SSL) of
	{ok, Bin} ->
	    ok = file:write(FD, Bin),
	    erlang:yield(),
	    upload_loop(CliSock, FD, Len-size(Bin), SegLen, SSL);
	_Err ->
	    exit(normal)
    end;
upload_loop(_CliSock, _FD, 0, _SegLen, _SSL) ->
    ?dbg("upload done\n", []),
    done.

min(X, Y) ->
    if
	X < Y ->
	    X;
	true ->
	    Y
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Upload a chunked-encoding request body
%%
%% Each chunk is loaded in a loop of SegLen-sized subchunks

upload_chunks(CliSock, FD, SegLen, SSL) ->
    case upload_chunk(CliSock, FD, SegLen, SSL) of
	final_chunk ->
	    done;
	interim_chunk_done ->
	    upload_chunks(CliSock, FD, SegLen, SSL)
    end.

upload_chunk(CliSock, FD, SegLen, SSL) ->
    N = get_chunk_length(CliSock, SSL),
    ?dbg("Chunk length: ~p\n", [N]),
    yaws:setopts(CliSock, [binary, {packet, raw}], SSL),
    if
	N == 0 ->
	    %% yaws:eat_crnl(CliSock, SSL),
	    ?dbg("final chunk, done\n", []),
	    final_chunk;
	N > 0 ->
	    upload_chunk_loop(CliSock, N, SegLen, FD, SSL)
    end.

upload_chunk_loop(CliSock, Len, SegLen, FD, SSL) when Len > 0 ->
    ReadLen = min(Len, SegLen),
    ?dbg("- read chunk ~p bytes\n", [ReadLen]),
    case yaws:cli_recv(CliSock, ReadLen, SSL) of
	{ok, Bin} ->
	    ok = file:write(FD, Bin),
	    erlang:yield(),
	    upload_chunk_loop(CliSock, Len-size(Bin), SegLen, FD, SSL);
	_Err ->
	    exit(normal)
    end;
upload_chunk_loop(CliSock, 0, _SegLen, _FD, SSL) ->
    yaws:eat_crnl(CliSock, SSL),
    ?dbg("- chunk done\n", []),
    interim_chunk_done.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NB. The chunk trailer (RFC 2616, 3.6.1) is only needed if the TE header
%% indicates "trailers" is acceptable.
%%   I think trailers only appear in the server response, if the client
%% accepts this with a "TE" header. Thus, NOT RELEVANT for uploads.
%%
%% 14.40: 0, 1 or more
%%   Trailer: header-name
%% indicating what headers will be sent in the trailer
%% 
%% 14.39: 0, 1, or more
%%   TE: trailers
%%   TE: transfer-extension [accept-params]

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Get the chunk length for chunked encoding. This appears as
%% a line "dddd[;...]CRLF"

get_chunk_length(CliSock, SSL) ->
    yaws:setopts(CliSock, [list, {packet, line}], SSL),
    case yaws:cli_recv(CliSock, 0, SSL) of
	{ok, Lst} ->
	    case catch chunk_len(Lst) of
		{'EXIT', _} ->
		    %% more useful to exit with the entire length
		    exit({bad_chunk_length, Lst});
		N ->
		    N
	    end;
	_Err ->
	    exit(normal)
    end.

chunk_len(Ds) ->
    chunk_len(Ds, 0).

chunk_len([D|Ds], Val) ->
    if
	D >= $0, D =< $9 ->
	    chunk_len(Ds, Val bsl 4 + (D-$0));
	D >= $A, D =< $F ->
	    chunk_len(Ds, Val bsl 4 + (10+D-$A));
	D >= $a, D =< $f ->
	    chunk_len(Ds, Val bsl 4 + (10+D-$a));
	D == $; ->
	    %% the interesting part of the chunk header is done
	    Val;
	D == $\r ->
	    Val;
	D == $\n ->
	    Val;
	true ->
	    %% has to be digits, semicolon or eol, else die
	    exit(bad_chunk_contents)
    end;
chunk_len([], _Val) ->
    %% line ended before newline ... I think
    exit(bad_chunk_end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% If the request has sent an "Expect: 100-continue" header, send 
%% an appropriate status code back, 100 if you want to continue.
%%
%% See also yaws_server:'POST'/3 and yaws_server:deliver_100/1 for
%% how this is handled in main yaws.
%%
%% UNFINISHED
%% - currently just responds with a 100, even though we could run
%%   auth and suchlike at this point
%% - we just look at the first of any Expect headers, there may be several
%%   ones

respond_to_100(CliSock, Head) ->
    Hdrs = Head#headers.other,
    case get_header("Expect", Hdrs) of
	not_found ->
	    ?dbg("Expect not found, skip\n", []),
	    ok;
	{found, Val = ("100" ++_)} ->
	    ?dbg("Expect found, ~p (sock: ~p)\n", [Val, CliSock]),
	    yaws:gen_tcp_send(CliSock, [<<"HTTP/1.1 100 Continue\r\n">>]);
	{found, Val} ->
	    ?dbg("Expect found, not 100: ~p\n", [Val]),
	    ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_header(Hdr, [ {http_header, _, Hdr, _, Val}|_]) ->
    {found, Val};
get_header(Hdr, [_|Hdrs]) ->
    get_header(Hdr, Hdrs);
get_header(Hdr, []) ->
    not_found.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Verify header constraints
%%
%% Yaws has special handling of some headers (e.g., content-length)

fold_headers(F, St, Arg) ->
    lists:foldl(F, St, Arg#arg.headers).

    
