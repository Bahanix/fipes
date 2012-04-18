-module(fipes_files).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/2]).


init({_Any, http}, Req, []) ->
    {ok, Req, []}.


handle(Req, State) ->
    {ok, Req2} = dispatch(Req),
    {ok, Req2, State}.


dispatch(Req) ->
    {Fipe, Req} = cowboy_http_req:binding(pipe, Req),
    case cowboy_http_req:method(Req) of
        {'GET', Req} ->
            case cowboy_http_req:binding(file, Req) of
                {undefined, Req} ->
                    index(Fipe, Req);
                {File, Req} ->
                    download(Fipe, File, Req)
            end;
        {'POST', Req} ->
            create(Fipe, Req)
    end.


index(Fipe, Req) ->
    Headers    = [{<<"Content-Type">>, <<"application/tnetstrings">>}],

    Objects    = ets:match_object(files, {{Fipe, '_'}, '_'}),
    FilesInfos = [{struct, FileInfos} ||
                     {{Fipe, _FileId}, {_Owner, FileInfos}} <- Objects],
    Results    = tnetstrings:encode(FilesInfos, [{label, atom}]),

    cowboy_http_req:reply(200, Headers, Results, Req).


% FIXME: Handle 404
download(Fipe, File, Req) ->
    % Register the downloader
    Uid = uid(),
    ets:insert(downloaders, {{Fipe, Uid}, self()}),

    io:format("Req: ~p~n", [Req]),

    Name  = name(Fipe, File),
    Range = range(Req),

    Headers =
        [
         {<<"Content-Type">>,        <<"application/octet-stream">>},
         {<<"Content-Disposition">>, [<<"attachment; filename=\"">>, Name, <<"\"">>]}
        ],

    Status =
        if
            Range == [] -> 200;
            true -> 206
        end,

    {ok, Req2} = cowboy_http_req:chunked_reply(200, Headers, Req),

    % Ask the file owner to start the stream
    owner(Fipe, File) ! {stream, File, Uid, Range},

    stream(Fipe, Uid, Req2).


owner(Fipe, File) ->
    [{{Fipe, File}, {Uid, _FileInfos}}] = ets:lookup(files, {Fipe, File}),
    [{{Fipe, Uid}, Owner}] = ets:lookup(owners, {Fipe, Uid}),
    Owner.

name(Fipe, File) ->
    [{{Fipe, File}, {_Uid, FileInfos}}] = ets:lookup(files, {Fipe, File}),
    proplists:get_value(name, FileInfos).

range(Req) ->
    case cowboy_http_req:header('Range', Req) of
        {undefined, Req2} -> [];
        {<<"bytes=", Range/binary>>, _Req2} ->
            [Start, End] = binary:split(Range, <<"-">>),
            Range2 = case End of
                         <<>> -> [];
                         _ ->
                             End2 = list_to_integer(binary_to_list(End)),
                             [{'end', End2}]
                     end,
            case Start of
                <<>> -> Range2;
                _ ->
                    Start2 = list_to_integer(binary_to_list(Start)),
                    [{start, Start2} | Range2]
            end
    end.


stream(Fipe, Uid, Req) ->
    receive
        {chunk, eos} ->
            ets:delete(downloaders, {Fipe, Uid}),
            {ok, Req};
        {chunk, Chunk} ->
            cowboy_http_req:chunk(Chunk, Req),
            stream(Fipe, Uid, Req)
    end.


create(Fipe, Req) ->
    {FileId, Owner, FileInfos} = file_infos(Req),
    true = ets:insert(files, {{Fipe, FileId}, {Owner, FileInfos}}),

    notify(Fipe, FileInfos),

    Headers = [{<<"Content-Type">>, <<"application/tnetstrings">>}],
    Result  = tnetstrings:encode({struct, FileInfos}),
    cowboy_http_req:reply(200, Headers, Result, Req).


file_infos(Req) ->
    FileId = fid(),

    {ok, Body, Req2} = cowboy_http_req:body(Req),
    {struct, FileInfos} = tnetstrings:decode(Body, [{label, atom}]),
    Owner = proplists:get_value(owner, FileInfos),

    {FileId, Owner, [{id, FileId}|FileInfos]}.

notify(Fipe, FileInfos) ->
    [Owner ! {new, FileInfos} || {{Fipe, Uid}, Owner} <- ets:tab2list(owners)],
    ok.


fid() ->
    {Mega, Sec, Micro} = erlang:now(),
    Timestamp = (Mega * 1000000 + Sec) * 1000000 + Micro,
    list_to_binary(integer_to_list(Timestamp, 16)).


terminate(_Req, _State) ->
    ok.


% XXX: duplicated code, see fipes_pipe:uid/0.
uid() ->
    {Mega, Sec, Micro} = erlang:now(),
    Timestamp = (Mega * 1000000 + Sec) * 1000000 + Micro,
    list_to_binary(integer_to_list(Timestamp, 16)).

