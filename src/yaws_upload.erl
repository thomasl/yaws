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
-export([body/3,body_discard/1]).
-export([upload_options/0,
	 set_upload_options/1]).

-include("../include/yaws_dav.hrl").
-include("../include/yaws_api.hrl").
-include("../include/yaws.hrl").
-include("yaws_debug.hrl").
-include_lib("kernel/include/file.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% -define(info(X,Y), error_logger:info_msg("~p:~p: " X,
%%                                         [?MODULE, ?LINE | Y])).
-define(tmpfile(File), ((File) ++ "_XXXXXX")).
-define(discard_path,"/opt/diino/erlang/diino_uploader/www_root/").

-define(read_segment_len, 8192).

%% The tempfile for uploads is opened using these options
%% - append is required
%% - raw and delayed_write are used to improve performance, but not
%%   strictly necessary
%%

-define(upload_app, yaws).
-define(upload_fd_options, upload_options()).
%% -define(upload_fd_defaults, [append, raw, delayed_write]).
-define(upload_fd_defaults, [append]).

-define(max_rename_retry, 3).
-define(retry_sleep, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% WARNING: setting upload options to something bad means big trouble, 
%% e.g., no tmpfiles can be opened.

set_upload_options(NewOpts) ->
    application:set_env(?upload_app, upload_file_options, NewOpts).

upload_options() ->
    case application:get_env(?upload_app, upload_file_options) of
	{ok, FD_Opts} ->  %% security by obscurity ...
	    FD_Opts;
	undefined ->
	    ?upload_fd_defaults
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% The body/3 method is invoked by yaws_server to complete an upload
%% (or reject it).
%%
%% NOTE:
%%  We use ?tmpfile() to ensure that the tmp file ends up in the same
%% filesystem as the destination file. This is done to avoid costly
%% cross-fs copying in some cases.
%%
%% UNFINISHED
%% - check that Content-Range header is not present
%%   * or handle it, file:pwrite/2
%% - measure size of process when done

body(ARG,PrevUsage,Vol) ->
    %% ?info("... upload body/3 ...\n",[]),
    CliSock = ARG#arg.clisock,
    Req = ARG#arg.req,
    Head = ARG#arg.headers,
    {User,_Pass,_Realm}=Head#headers.authorization,
    SC = get(sc),
    SSL = yaws:is_ssl(SC),
    ok = yaws:setopts(CliSock, [{packet, raw}, binary], SSL),
    PPS = SC#sconf.partial_post_size,
    %% open tempfile as FD
    %% if upload fails, delete tempfile
    %% when upload completes, move tempfile to destfile 
    %%
    %% Docroot not needed
    DocRoot = ARG#arg.docroot,
    File = file_of_url(DocRoot, Req),
    PrevExist = file_exists(File),
    %% the seed/0 should be done at startup (start of application?)
    yaws_tmpfile:seed(),
    %% Note: PrevEx0 can be used to control behaviour of when tempfiles
    %%   are created or not, but should normally be PrevEx0 = PrevExist
    PrevEx0 = PrevExist,
    {TmpFile, FD} = upload_to_file(File, PrevEx0),
    %% ?info("upload ~p -> tmpfile ~p\n", [Req#http_request.path, TmpFile]),
    %% Note: 
    try 
	case respond_to_100(CliSock, Head) of
	    ok ->
		case Head#headers.content_length of
		    undefined ->
			case Head#headers.transfer_encoding of
			    "chunked" ->
				%% transfer in chunks
				%% ?info("chunked transfer\n", []),
				SegLen = upload_segment_length(PPS),
				Length=upload_chunks(CliSock, FD, SegLen, SSL),
				%% %% ?info("Length: ~p~n",[Length]),
				final_rename(CliSock, Req, Length, 
					     User, PrevUsage, Vol,
					     PrevEx0, FD, TmpFile, File);
			    Enc ->
				%% ?error("Unknown transfer-encoding: ~p\n", 
				%% [Enc]),
				deliver_500(CliSock, Req)
			end;
		    Len_lst ->
			case catch list_to_integer(Len_lst) of
			    Length when is_integer(Length) ->
				%% ?info("content-length: ~p\n", [Length]),
				SegLen = upload_segment_length(PPS),
				upload_body_length(
				  CliSock, FD, Length, SegLen, SSL
				 ),
				final_rename(CliSock, Req, Length, 
					     User, PrevUsage, Vol,
					     PrevEx0, FD, TmpFile, File);
			    {'EXIT', _} ->
				%% ?error("Bad content-length: '~s'\n",
				%% [Len_lst]),
				deliver_500(CliSock, Req)
			end
		end;
	    Err ->
		%% final response has been sent, done
		%% ?error("error respond_to_100: ~p\n", [Err]),
		deliver_500(CliSock, Req)
	end
    catch
	_:normal ->
	    %% indicates completed normally
	    %% ?info("~p exit normal, ok\n", [self()]),
	    ok;
	Type:Exn ->
	    %% ?error("Exception ~p:~p when uploading ~p -> ~p\n",
	    %% [Type, Exn, Req#http_request.path, TmpFile]),
	    deliver_500(CliSock, Req)
    after
	%% always close and delete the tempfile when done
	%% - close the fd regardless of outcome, may already be closed
	%%   but file:close/1 is okay with that
	file:close(FD),
      case PrevExist of
	  true ->
	      %% ?info("PrevExist true, kill tmpfile ~s (if any)\n", [TmpFile]),
	      case file:delete(TmpFile) of
		  ok ->
		      %% ?error("Tmpfile still exists but "
		      %% "should have been renamed: deleted it: ~p\n", 
		      %% [TmpFile]);
		      ok;
		  _ ->
		      %% delete fails if file has been renamed, which means
		      %% upload okay
		      %% ?info("No tmpfile, request done\n", []),
		      ok
	      end;
	  false ->
	      %% ?info("PrevExist false, request done\n", []),
	      ok
      end
    end.

%% Select the destination file for upload and open it with suitable
%% options.
%%
%% If the File exists, open a tempfile and upload to that. If the upload
%% succeeds, rename TmpFile -> File (done elsewhere). Otherwise delete it.
%%
%% If the File does not exist, use it as destination directly.

upload_to_file(File, true) ->
    %% file already exists, avoid overwriting it
    %% try to open tempfile, which may fail/exit
    case yaws_tmpfile:open(File, ?upload_fd_options) of 
	{ok, TmpFile_0, FD_0} ->
	    {TmpFile_0, FD_0};
	OpenErr ->
	    %% UNFINISHED
	    %% - or do "mkdir -p" (for /bak)
	    %% ?error("Unable to open tmpfile for ~p: ~p", 
	    %% [File, OpenErr]),
	    exit({{?MODULE, ?LINE}, OpenErr, File})
    end;
upload_to_file(File, false) ->
    %% If file doesn't exist, tmpfile = destination file
    case file:open(File, ?upload_fd_options) of
	{ok, FD_0} ->
	    {File, FD_0};
	OpenErr ->
	    case filelib:ensure_dir(File) of
		ok ->
		    case file:open(File, ?upload_fd_options) of
			{ok, FD_0} ->
			    {File, FD_0};
			OpenErr_2 ->
			    %% ?error("Error ~p open(2), file ~s\n",
			    %% [OpenErr_2, File]),
			    exit({{?MODULE, ?LINE}, OpenErr, File})
		    end;
		EnsureErr ->
		    %% ?error("Error ~p on ensure_dir ~p\n",
		    %% [EnsureErr, File]),
		    exit({{?MODULE, ?LINE}, EnsureErr, File})
	    end
    end.

%% This refactors the final rename operation after upload to the
%% temporary file has been finished.
%%
%% Yes, the interface is very messy at this point ... Maybe we
%% ought to pass ARG and extract the parts instead? Ugh.
%%
%% UNFINISHED
%% - handle Content-MD5 or X-Content-SHA256

final_rename(CliSock, Req, _Content_length, 
	     _User, _PrevUsage, _Vol, 
	     _PrevExist = false, _FD, _TmpFile, File) ->
    %% In this case, no renaming needed
    %% ?info("File ~s uploaded (no rename)\n", [File]),
    deliver_201(CliSock, Req);
final_rename(CliSock, Req, Content_length, 
	     User, PrevUsage, Vol, 
	     PrevExist = true, FD, TmpFile, File) ->
    final_rename_no_retry(CliSock, Req, Content_length, 
		       User, PrevUsage, Vol, 
		       PrevExist, FD, TmpFile, File).

final_rename_no_retry(CliSock, Req, Content_length, 
		      User, PrevUsage, Vol, 
		      PrevExist, _FD, TmpFile, File) ->
    %% ?info("rename TRY ~p -> ~p\n", [TmpFile, File]),
    %% file:rename/2 should be "retrying" + "checking"
    case (catch file:rename(TmpFile, File)) of
	ok ->
	    %% - should optionally also verify that Content_length
	    %%   equals file size
	    %% ?info("rename OK: ~p -> ~p\n", [TmpFile, File]),
	    %% finally, generate the response
	    case PrevExist of
		true ->
		    deliver_204(CliSock, Req);
		false ->
		    deliver_201(CliSock, Req)
	    end;
	{error, Err} ->
	    %% ?error("Error ~p on rename ~p -> ~p\n", 
	    %% [file:format_error(Err), TmpFile, File]),
	    deliver_500(CliSock, Req);
	Err ->
	    %% ?error("Error 2: ~p on rename ~p -> ~p\n", 
	    %% [(Err), TmpFile, File]),
	    deliver_500(CliSock, Req)
    end.

%% Same as above, use "retrying rename"

final_rename_retry(CliSock, Req, Content_length, 
		   User, PrevUsage, Vol, 
		   PrevExist, _FD, TmpFile, File) ->
    %% - do we need to sync the fd before closing? gah ... NO
    %% - do we need to close the fd before renaming? MAYBE
    %% file:close(FD)
    %% ?info("[retry] rename ~p -> ~p\n", [TmpFile, File]),
    case retry_rename(TmpFile, File) of
	ok ->
	    %% finally, generate the response
	    case PrevExist of
		true ->
		    deliver_204(CliSock, Req);
		false ->
		    deliver_201(CliSock, Req)
	    end;
	{error, Err} ->
	    %% ?error("Error ~p on rename ~p -> ~p\n", 
	    %% [file:format_error(Err), TmpFile, File]),
	    deliver_500(CliSock, Req)
    end.

%% The following is just done for effect

verify_file_size(Len, File) ->
    catch verify_file_size_0(Len, File).

verify_file_size_0(Len, File) ->
    case file:read_file_info(File) of
	{ok, Info } ->
	    Size = Info#file_info.size,
	    if
		Len == Size ->
		    %% ?info("File ~p has proper length ~p\n",
		    %% [File, Len]);
		    ok;
		true ->
		    %% ?error("FILE SIZE ERROR: File ~p: content-length ~p, but "
			   %% "fs length ~p\n",
			   %% [File, Len, Size])
		    ok
	    end;
	Err ->
	    %% ?error("Error verifying size of uploaded file ~p: ~p\n",
	    %% [File, Err])
	    ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Solid renaming that handles various sorts of badness

retry_rename(TmpFile, File) ->
    retry_rename(?max_rename_retry, ?retry_sleep, TmpFile, File).

retry_rename(0, _S, TmpFile, File) ->
    %% should we accumulate the previous errors?
    {error, {unable_to_rename, TmpFile, File}};
retry_rename(N, S, TmpFile, File) when N > 0 ->
    case (catch file:rename(TmpFile, File)) of
	ok ->
	    %% Rename succeeded
	    ok;
	{error, enoent} ->
	    %% tmpfile? directory of file?
	    %% - analyze this, then redo
	    %% ?error("Rename failed ENOENT [retry ~p times more]: ~p -> ~p\n",
	    %% [N, TmpFile, File]),
	    analyze_enoent(TmpFile, File),
	    sleep(S),
	    retry_rename(N-1, S, TmpFile, File);
	{'EXIT', Rsn} ->
	    %% analyze, log
	    %% ?error("Rename exited ~p [retry ~p times more]: ~p -> ~p\n",
	    %% [Rsn, N, TmpFile, File]),
	    sleep(S),
	    retry_rename(N-1, S, TmpFile, File);
	OtherErr ->
	    %% analyze, log
	    %% ?error("Rename error ~p [retry ~p times more]: ~p -> ~p\n",
	    %% [OtherErr, N, TmpFile, File]),
	    sleep(S),
	    retry_rename(N-1, S, TmpFile, File)
    end.

sleep(N) when is_integer(N), N > 0 ->
    receive
    after N ->
	    ok
    end.

%% Try to figure out the reason for the enoent

-include_lib("kernel/include/file.hrl").

analyze_enoent(_FromF, _ToF) ->
    %% case file:read_file_info(
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Read and discard the request entity body.
%%
%% This is basically just a modification of code above. Can be simplified
%% a great deal if so desired.
%%
%% NOTE: some clients seem not to handle an early response from the server
%%  (ie, a response arrives before the request entity body has been sent).
%%  For compatibility with these clients, we run the following to read and
%% discard the request entity body.
%%
%% I think clients ought in principle accept early responses (ie, getting
%% a response before sending the entire body). For instance, the 100-continue
%% implies this. Not sure if that's required though.

body_discard(ARG) ->
    %% ?info("... upload body_discard/1 ...\n", []),
    CliSock = ARG#arg.clisock,
    Req = ARG#arg.req,
    Head = ARG#arg.headers,
%    {User,_Pass,_Realm}=Head#headers.authorization,
    SC = get(sc),
    SSL = yaws:is_ssl(SC),
    ok = yaws:setopts(CliSock, [{packet, raw}, binary], SSL),
    PPS = SC#sconf.partial_post_size,
    %% open tempfile as FD
    %% if upload fails, delete tempfile
    %% when upload completes, move tempfile to destfile 
    DocRoot = ARG#arg.docroot,
%    File = file_of_url(DocRoot, Req),
    File = DocRoot ++ "/discard",
    PrevExist = file_exists(File),
    %% the seed/0 should be done at startup (start of application?)
    yaws_tmpfile:seed(),
    {ok, TmpFile, FD} = 
	yaws_tmpfile:open(DocRoot ++ "/discard_XXXXXX", [append, raw, delayed_write]),
    %% ?info("upload ~p -> tmpfile ~p\n", [Req#http_request.path, TmpFile]),
    %% Note: 
    try 
	case respond_to_100(CliSock, Head) of
	    ok ->
		case Head#headers.content_length of
		    undefined ->
			case Head#headers.transfer_encoding of
			    "chunked" ->
				%% transfer in chunks
				%% ?info("chunked transfer\n", []),
				SegLen = upload_segment_length(PPS),
				Length=upload_chunks(CliSock, FD, SegLen, SSL),
				%% ?info("Length: ~p~n",[Length]),
				%% ?info("rename ~p -> ~p\n", [TmpFile, File]),
				ok = file:rename(TmpFile, File),
				%% Update quota
				%% quota:inc_usage(User,Length),
				case PrevExist of
				    true ->
					%% ?info("DONE. File updated. no 204~n",[]);
					deliver_204(CliSock, Req);
				    false ->
					%% ?info("DONE. File created. no 201~n",[])
					deliver_201(CliSock, Req)
				end;
			    Enc ->
				%% ?error("Unknown transfer-encoding: ~p\n", 
				%% [Enc]),
				%% ?error("Unknown transfer-encoding: ~p\n", 
				%% [Enc]),
				deliver_500(CliSock, Req)
			end;
		    Len_lst ->
			Len = list_to_integer(Len_lst),
			%% ?info("content-length: ~p\n", [Len]),
			SegLen = upload_segment_length(PPS),
			upload_body_length(
			  CliSock, FD, Len, SegLen, SSL
			 ),
			%% ?info("rename ~p -> ~p\n", [TmpFile, File]),
			case (catch file:rename(TmpFile, File)) of
			    ok ->
				%% Update quota
				%% quota:inc_usage(User,Head#headers.content_length),
				case PrevExist of
				    true ->
					%% ?info("DONE. File updated. no 204~n",[]);
					deliver_204(CliSock, Req);
				    false ->
					%% ?info("DONE. File created. no 201~n",[])
					deliver_201(CliSock, Req)
				end;
			    {error, Err} ->
				%% ?error("Error ~p on rename ~p -> ~p\n", 
				%% [file:format_error(Err), TmpFile, File]),
				deliver_400(CliSock, Req);
			    Err ->
				%% ?error("Error 2: ~p on rename ~p -> ~p\n", 
				%% [(Err), TmpFile, File]),
				deliver_400(CliSock, Req)
			end
		end;
	    Err ->
		%% final response has been sent, done
		%% ?error("error respond_to_100: ~p\n", [Err]),
		deliver_500(CliSock, Req)
	end
    catch
	Type:Exn ->
	    %% ?error("Exception ~p:~p when uploading ~p -> ~p\n",
	    %% [Type, Exn, Req#http_request.path, TmpFile]),
	    deliver_500(CliSock, Req)
    after
	%% always close and delete the tempfile when done
	file:close(FD),
        case file:delete(TmpFile) of
	    ok ->
		%% ?error("Upload failed, deleted tmpfile ~p\n", [TmpFile]);
		ok;
	    _ ->
		%% delete fails if file has been renamed, which means
		%% upload okay
		%% ?info("Upload done\n", []),
		ok
	end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Just invoke deliver_xxx for the various responses. We append CRLF
%% to play well with haproxy (hopefully)

%% "Created"

deliver_201(CliSock, Req) ->
    %% ?info("Deliver 201\n", []),
    yaws_server:deliver_xxx(CliSock, Req, 201, "\r\n").
    
%% "Updated"

deliver_204(CliSock, Req) ->
    %% ?info("Deliver 204\n", []),
    yaws_server:deliver_xxx(CliSock, Req, 204, "\r\n").
    
%% "Forbidden"

deliver_400(CliSock, Req) ->
    %% ?info("Deliver 400\n", []),
    yaws_server:deliver_xxx(CliSock, Req, 400, "\r\n").

%% "Internal Server Error"

deliver_500(CliSock, Req) ->
    %% ?info("Deliver 500\n", []),
    yaws_server:deliver_xxx(CliSock, Req, 500, "\r\n").

%% "Not Implemented" (currently unused)

deliver_501(CliSock, Req) ->
    %% ?info("Deliver 501\n", []),
    yaws_server:deliver_xxx(CliSock, Req, 501, "\r\n").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% The segment length is the size of each chunk read from the socket
%% (not to be confused with chunked encoding). This is defined by the
%% partial_post_size parameter if available, otherwise set to a "sensible
%% default".

upload_segment_length(nolimit) ->
    ?read_segment_len;
upload_segment_length(N) when is_integer(N), N > 0 ->
    N;
upload_segment_length(X) ->
    exit({unknown_post_size_value, X}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

file_of_url(_DocRoot, Req) ->
    case Req#http_request.path of
	{abs_path, Path} ->
%	    Tempfile = DocRoot ++ Path,
	    Tempfile = Path,
	    %% ?info("Path = ~p, temp = ~p\n", [Path, Tempfile]),
	    Tempfile;
	Err ->
	    %% ?error("UNFINISHED - Unable to map ~p\n", [Err])
	    %% exit?
	    ""
    end.

file_exists(File) ->
    case file:read_file_info(File) of
	{ok, _Finfo} ->
	    %% check permissions? e.g., write perm
	    %% for a web server, you generally own all the data
	    %% so probably not needed
	    true;
	{error, enoent} ->
	    false;
	{error, Other} ->
	    %% ?error("File check for ~s: error ~p\n", [File, Other]),
	    false
    end.

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
%%    ?dbg("wait for ~p bytes ...\n", [ReadLen]),
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
    %% ?info("upload done\n", []),
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
%%
%% The total length is accumulated in the last parameter, TotalLength,
%% and returned.

upload_chunks(CliSock, FD, SegLen, SSL) ->
    upload_chunks(CliSock, FD, SegLen, SSL, 0).
    
upload_chunks(CliSock, FD, SegLen, SSL, TotalLength) ->
%%    %% ?info("TotalLength: ~p~n",[TotalLength]),
    case upload_chunk(CliSock, FD, SegLen, SSL, TotalLength) of
	{final_chunk, RecTotalLength} ->
	    %% Returns Content-Length when ready
	    RecTotalLength;
	{interim_chunk_done, RecTotalLength} ->
	    upload_chunks(CliSock, FD, SegLen, SSL, RecTotalLength)
    end.

upload_chunk(CliSock, FD, SegLen, SSL, TotalLength) ->
    N = get_chunk_length(CliSock, SSL),
%%    ?info("Chunk length: ~p\nSegLen: ~p~n", [N,SegLen]),
    yaws:setopts(CliSock, [binary, {packet, raw}], SSL),
    if
	N == 0 ->
%%	    %% yaws:eat_crnl(CliSock, SSL),
	    %% ?info("final chunk, done\n", []),
	    {final_chunk,(TotalLength+N)};
	N > 0 ->
	    %% ?info("Call upload_chunk_loop (chunk ~p bytes)",[N]),
	    upload_chunk_loop(CliSock, N, SegLen, FD, SSL, (TotalLength+N))
    end.

upload_chunk_loop(CliSock, Len, SegLen, FD, SSL, TotalLength) when Len > 0 ->
    ReadLen = min(Len, SegLen),
%%    %% ?info("- read chunk ~p bytes\n", [ReadLen]),
    case yaws:cli_recv(CliSock, ReadLen, SSL) of
	{ok, Bin} ->
	    ok = file:write(FD, Bin),
	    erlang:yield(),
	    upload_chunk_loop(CliSock, Len-size(Bin), SegLen, FD, SSL, TotalLength);
	_Err ->
	    exit(normal)
    end;
upload_chunk_loop(CliSock, 0, _SegLen, _FD, SSL, TotalLength) ->
    yaws:eat_crnl(CliSock, SSL),
%%    ?info("- chunk done\n", []),
    {interim_chunk_done, TotalLength}.

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
	    %% ?info("'Expect' header not found, skip\n", []),
	    ok;
	{found, Val = ("100" ++_)} ->
	    %% ?info("'Expect' header found, ~p (sock: ~p)\n", [Val, CliSock]),
	    yaws:gen_tcp_send(CliSock, [<<"HTTP/1.1 100 Continue\r\n">>]);
	{found, Val} ->
	    %% ?info("'Expect' header found but not 100, skip: ~p\n", [Val]),
	    ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Returns not_found | {found, MD5_header_value}

has_content_md5(Head) ->
    Hdrs = Head#headers.other,
    get_header("Content-MD5", Hdrs).
	    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_header(Hdr, [ {http_header, _, Hdr, _, Val}|_]) ->
    {found, Val};
get_header(Hdr, [_|Hdrs]) ->
    get_header(Hdr, Hdrs);
get_header(_Hdr, []) ->
    not_found.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Verify header constraints
%%
%% Yaws has special handling of some headers (e.g., content-length)

fold_headers(F, St, Arg) ->
    lists:foldl(F, St, Arg#arg.headers).
