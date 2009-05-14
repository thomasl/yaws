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

-define(read_segment_len, 8192).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% The body/3 method is invoked by yaws_server to complete an upload
%% (or reject it).

body(CliSock, Req, Head) ->
    io:format("... body_method ...\n", []),
    SC = get(sc),
    SSL = yaws:is_ssl(SC),
    ok = yaws:setopts(CliSock, [{packet, raw}, binary], SSL),
    PPS = SC#sconf.partial_post_size,
    io:format("... content-length = ~p, te = ~p\n", 
	      [Head#headers.content_length, 
	       Head#headers.transfer_encoding]),
    io:format("Headers: ~p\n", [Head]),
    %% open tempfile as FD
    %% if upload fails, delete tempfile
    %% when upload completes, move tempfile to destfile 
    File = file_of_url(Req),
    {ok, FD} = file:open(File, [append, raw, delayed_write]),
    case respond_to_100(CliSock, Head) of
	ok ->
	    case Head#headers.content_length of
		undefined ->
		    case Head#headers.transfer_encoding of
			"chunked" ->
			    %% transfer in chunks	
			    io:format("chunked transfer\n", []),
			    case PPS of
				nolimit ->
				    %% read chunk at a time
				    io:format("PPS: nolimit\n", []),
				    SegLen = ?read_segment_len,
				    upload_chunks(CliSock, FD, SegLen, SSL);
				SegLen when integer(SegLen), SegLen > 0 ->
				    %% read chunks,
				    %% read parts of chunk in loop
				    io:format("PPS: ~p\n", [SegLen]),
				    upload_chunks(CliSock, FD, SegLen, SSL);
				_ ->
				    %% unknown config, die
				    io:format("PPS bad: ~p\n", [PPS]),
				    exit(nyi)
			    end;
			_ ->
			    %% unknown transfer method, die
			    io:format("", []),
			    exit(nyi)
		    end;
		Len_lst ->
		    case (catch list_to_integer(Len_lst)) of
			{'EXIT', Rsn} ->
			    io:format("Bad length: ~p\n", [Len_lst]),
			    exit(nyi);
			Len ->
			    io:format("content-length: ~p\n", [Len]),
			    case PPS of
				nolimit ->
				    %% read entity-body at once
				    io:format("PPS-size: nolimit\n", []),
				    SegLen = ?read_segment_len,
				    upload_body_length(
				      CliSock, FD, Len, SegLen, SSL
				     );
				SegLen when integer(SegLen), SegLen > 0 ->
				    %% read parts in loop 
				    io:format("PPS-size ~p\n", [SegLen]),
				    upload_body_length(
				      CliSock, FD, Len, SegLen, SSL
				     );
				_ ->
				    %% unknown config
				    io:format("unknown post-size: ~p\n", [PPS]),
				    exit(nyi)
			    end
		    end
	    end;
	{error, Rsn} ->
	    %% final response has been sent, done
	    io:format("respond_to_100: ~p\n", [Rsn]),
	    exit({nyi, Rsn})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% UNFINISHED
%% - resolve to actual fs path
%% - e.g., use docroot ++ path for abspath
%%   or a "map to storage handler"

file_of_url(Req) ->
    Path = Req#http_request.path,
    Tempfile = "/home/thomasl/foo",
    io:format("Path = ~p, temp = ~p\n", [Path, Tempfile]),
    Tempfile.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% All uploads are run as loops, reading at most a limited number of
%% bytes before shuttling them to the output fd.
%%
%% There are a number of variations on this theme:
%% - body length known
%% - body length unknown
%% - body uses chunked transfer-encoding

upload_body_length(CliSock, FD, Len, SegLen, SSL) ->
    yaws:setopts(CliSock, [binary, {packet, raw}], SSL),
    upload_loop(CliSock, FD, Len, SegLen, SSL).

upload_loop(CliSock, FD, Len, SegLen, SSL) when Len > 0 ->
    ReadLen = min(Len, SegLen),
    io:format("wait for ~p bytes ...\n", [ReadLen]),
    % case yaws:do_recv(CliSock, SegLen, SSL) of
    Read_timeout = 3000,
    case gen_tcp:recv(CliSock, ReadLen, Read_timeout) of
	{ok, Bin} ->
	    ok = file:write(FD, Bin),
	    upload_loop(CliSock, FD, Len-size(Bin), SegLen, SSL);
	_Err ->
	    exit(normal)
    end;
upload_loop(_CliSock, _FD, 0, _SegLen, _SSL) ->
    io:format("upload done\n", []),
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
%% Upload a request body until EOF

upload_body_all(CliSock, FD, SegLen, SSL) ->
    case yaws:cli_recv(CliSock, SegLen, SSL) of
	{ok, Bin} ->
	    ok = file:write(FD, Bin),
	    upload_body_all(CliSock, FD, SegLen, SSL);
	_Err ->
	    exit(normal)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Upload a chunked-encoding request body
%%
%% NB: this is 

upload_chunks(CliSock, FD, SegLen, SSL) ->
    case upload_chunk(CliSock, FD, SegLen, SSL) of
	done ->
	    done;
	ok ->
	    upload_chunks(CliSock, FD, SegLen, SSL)
    end.

upload_chunk(CliSock, FD, SegLen, SSL) ->
    yaws:setopts(CliSock, [list, {packet, line}], SSL),
    N = get_chunk_length(CliSock, SSL),
    yaws:setopts(CliSock, [binary, {packet, raw}], SSL),
    if
	N == 0 ->
	    yaws:eat_crnl(CliSock, SSL),
	    done;
	N > 0 ->
	    upload_chunk_loop(CliSock, N, SegLen, FD, SSL)
    end.

upload_chunk_loop(CliSock, Len, SegLen, FD, SSL) when Len > 0 ->
    case yaws:cli_recv(CliSock, SegLen, SSL) of
	{ok, Bin} ->
	    ok = file:write(FD, Bin),
	    upload_chunk_loop(CliSock, Len-size(Bin), SegLen, FD, SSL);
	_Err ->
	    exit(normal)
    end;
upload_chunk_loop(_CliSock, 0, _SegLen, _FD, _SSL) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% The chunk trailer (RFC 2616, 3.6.1) is only needed if the TE header
%% indicates "trailers" is acceptable.
%%   I think trailers only appear in the server response, if the client
%% accepts this with a "TE" header.
%%
%% 14.40: 0, 1 or more
%%   Trailer: header-name
%% indicating what headers will be sent in the trailer
%% 
%% 14.39: 0, 1, or more
%%   TE: trailers
%%   TE: transfer-extension [accept-params]
%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Get the chunk length for chunked encoding.
%%
%% Requires socket in line/list mode.

get_chunk_length(CliSock, SSL) ->
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
%% UNFINISHED
%% - currently just responds with a 100, even though we could run
%%   auth and suchlike at this point
%% - we just look at the first of any Expect headers, there may be several
%%   ones

respond_to_100(CliSock, Head) ->
    Hdrs = Head#headers.other,
    case get_header("Expect", Hdrs) of
	not_found ->
	    io:format("Expect not found, skip\n", []),
	    ok;
	{found, Val = ("100" ++_)} ->
	    io:format("Expect found, ~p (sock: ~p)\n", [Val, CliSock]),
	    yaws:gen_tcp_send(CliSock, [<<"HTTP/1.1 100 Continue\r\n">>]);
	{found, Val} ->
	    io:format("Expect found, not 100: ~p\n", [Val]),
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

    
