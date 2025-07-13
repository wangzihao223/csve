-module(example).

-export([main/0, mult_process/1, write_csv/0, copy_csv/0]).

main() ->
  loop2(1).

mult_process(0) ->
  ok;
mult_process(N) ->
  spawn(fun() -> loop2(10) end),
  mult_process(N - 1).

write_csv() ->
  PrivDir = code:priv_dir(csve),
  FilePath = filename:join(PrivDir, "test3_write.csv"),
  {ok, Fd} = file:open(FilePath, [write]),

  Data = [["name", "age", "addr"], ["wang\"zi\"hao", 18, hebei]],
  Lines = csve:write(Data),
  ok = file:write(Fd, Lines),
  ok = file:close(Fd).

copy_csv() ->
  PrivDir = code:priv_dir(csve),
  FilePath = filename:join(PrivDir, "test5.csv"),
  {ok, Parse} = csve:csv_new(),
  FilePath1 = filename:join(PrivDir, "test5_copy.csv"),
  {ok, Fd} = file:open(FilePath1, [write]),
  stream_read1(FilePath, Parse, Fd).

loop2(0) ->
  ok;
loop2(N) ->
  PrivDir = code:priv_dir(csve),
  _FilePath = filename:join(PrivDir, "test3.csv"),
  _FilePath1 = filename:join(PrivDir, "test2.csv"),
  FilePath2 = filename:join(PrivDir, "test.csv"),
  {ok, Parse} = csve:csv_new(),
  loop([FilePath2], Parse),
  csve:csv_close(Parse),
  loop2(N - 1).

loop([], _Parse) ->
  ok;
loop([F | Next], Parse) ->
  stream_read(F, Parse),
  loop(Next, Parse).

stream_read(Filename, Parse) ->
  {ok, Io} = file:open(Filename, [read, raw, binary]),
  loop_read(Io, Parse, 0),
  file:close(Io).

loop_read(Io, Parse, Row) ->
  case file:read(Io, 128) of
    {ok, Bin} ->
      %% 处理 Bin（可能不是完整行）
      R = csve:csv_parse_chunk(Parse, Bin),
      {ok, Res} = R,
      Row1 = length(Res) + Row,
      io:format("Read chunk: ~p~n", [Res]),
      loop_read(Io, Parse, Row1);
    eof ->
      Res = csve:csv_fini(Parse),
      io:format("end ~p~n", [Res]),
      io:format("end ~p~n", [Row]);
    {error, Reason} ->
      io:format("Error: ~p~n", [Reason])
  end.

stream_read1(Filename, Parse, Fd) ->
  {ok, Io} = file:open(Filename, [read, raw, binary]),
  loop_read1(Io, Parse, Fd, 0),
  file:close(Io).

loop_read1(Io, Parse, Fd, Row) ->
  case file:read(Io, 128) of
    {ok, Bin} ->
      %% 处理 Bin（可能不是完整行）
      R = csve:csv_parse_chunk(Parse, Bin),
      {ok, Res} = R,
      Lines = csve:write(Res),
      ok = file:write(Fd, Lines),

      Row1 = length(Res) + Row,
      %io:format("Read chunk: ~p~n", [Res]),
      loop_read1(Io, Parse, Fd, Row1);
    eof ->
      Res = csve:csv_fini(Parse),
      io:format("end ~p~n", [Res]);
    {error, Reason} ->
      io:format("Error: ~p~n", [Reason])
  end.
