# csve


An read and write csv library

封装了 libcsv https://github.com/rgamble/libcsv， 实现以下方法

````erlang
% 类型
-type csv_parser() :: reference().
-type csv_option() :: #{delim => char(), quote => char()}.
-type csv_rows() :: [[binary()]].

csv_new() -> csv_parser().

csv_parse_chunk(Handle, Bin) -> {ok, csv_rows()}
  when Handle :: csv_parser(),
       Bin :: binary().

csv_fini(Handle) -> {ok, csv_rows()} | {error, string()}
  when Handle :: csv_parser().

csv_close(Handle :: csv_parser()) -> ok.

write([[term()]]) -> iolist().

write([[term()]], csv_option()) -> iolist().
````



## Build

-----

    $ rebar3 compile

## example

在项目目录下新建`priv`文件夹，放入测试`test.csv`

```erlang
parse_csv()->  
  PrivDir = code:priv_dir(csve),
  FilePath = filename:join(PrivDir, "test.csv"),
  {ok, Parse} = csve:csv_new(), % 新建一个csv 解析器
  stream_read(FilePath, Parse),
  % 完成解析任务关闭解析器
  csve:csv_close(Parse),
    
stream_read(Filename, Parse) ->
  {ok, Io} = file:open(Filename, [read, raw, binary]),
  loop_read(Io, Parse, 0),
  file:close(Io).

loop_read(Io, Parse, Row) ->
  case file:read(Io, 128) of
    {ok, Bin} ->
      %% 处理 Bin（可能不是完整行）
	  %% 流式的解析一个chunk, 解析器会缓存解析状态
      R = csve:csv_parse_chunk(Parse, Bin),
      {ok, Res} = R,
      Row1 = length(Res) + Row,
      io:format("Read chunk: ~p~n", [Res]),
      loop_read(Io, Parse, Row1);
    eof ->
	  % 解析完成一个文件重置csv解析器， 可重复利用
      Res = csve:csv_fini(Parse),
      io:format("end ~p~n", [Res]),
      io:format("end ~p~n", [Row]);
    {error, Reason} ->
      io:format("Error: ~p~n", [Reason])
  end.
```

