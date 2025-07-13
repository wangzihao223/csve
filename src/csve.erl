-module(csve).

-export([csv_new/0, csv_parse_chunk/2, csv_fini/1, csv_close/1]).

-on_load init/0.

-export([write/1, write/2]).

-define(DEFAULT_DELIM, $,).   % ASCII for ,
-define(DEFAULT_QUOTE, $").   % ASCII for "
-define(DEFAULT_LINE_END, <<"\r\n">>).

-type csv_parser() :: reference().
-type csv_option() :: #{delim => char(), quote => char()}.
-type csv_rows() :: [[binary()]].

init() ->
  PrivDir = code:priv_dir(csve),
  NifPath = filename:join(PrivDir, "csve"),
  case erlang:load_nif(NifPath, 0) of
    ok ->
      ok;
    {error, Reason} ->
      exit({load_nif_failed, Reason})
  end.

-spec csv_new() -> csv_parser().
csv_new() ->
  erlang:nif_error(undef).

-spec csv_parse_chunk(Handle, Bin) -> {ok, csv_rows()}
  when Handle :: csv_parser(),
       Bin :: binary().
csv_parse_chunk(_Handle, _Bin) ->
  erlang:nif_error(undef).

-spec csv_fini(Handle) -> {ok, csv_rows()} | {error, string()}
  when Handle :: csv_parser().
csv_fini(_Handle) ->
  erlang:nif_error(undef).

-spec csv_close(Handle :: csv_parser()) -> ok.
csv_close(_Handle) ->
  erlang:nif_error(undef).

-spec write([[term()]]) -> iolist().
write(Rows) ->
  write(Rows, #{}).

-spec write([[term()]], csv_option()) -> iolist().
write(Rows, Opts) ->
  Delim = maps:get(delim, Opts, ?DEFAULT_DELIM),
  Quote = maps:get(quote, Opts, ?DEFAULT_QUOTE),
  LineEnd = ?DEFAULT_LINE_END,
  Lines = [write_row(Row, Delim, Quote, LineEnd) || Row <- Rows],
  Lines.

write_row(Fields, Delim, Quote, LineEnd) ->
  Escaped = [escape_field(F, Delim, Quote) || F <- Fields],
  iolist_join(Escaped, <<Delim>>, LineEnd).

escape_field(Field, Delim, Quote) ->
  Bin = to_binary(Field),
  case needs_quote(Bin, Delim, Quote) of
    true ->
      Q = <<Quote>>,
      Escaped = binary:replace(Bin, Q, <<Quote, Quote>>, [global]),
      [Q, Escaped, Q];
    false ->
      Bin
  end.

needs_quote(Bin, Delim, Quote) ->
  BinList = binary_to_list(Bin),
  contains(BinList, Delim)
  orelse contains(BinList, Quote)
  orelse contains(BinList, $\n)
  orelse contains(BinList, $\r)
  orelse starts_or_ends_with_space(BinList).

contains(List, Char) ->
  lists:member(Char, List).

starts_or_ends_with_space([]) ->
  false;
starts_or_ends_with_space([H | T]) ->
  lists:member(H, [$\s, $\t])
  orelse lists:last([H | T]) == $\s
  orelse lists:last([H | T]) == $\t.

to_binary(Field) when is_binary(Field) ->
  Field;
to_binary(Field) when is_list(Field) ->
  unicode:characters_to_binary(Field);
to_binary(Field) when is_integer(Field) ->
  integer_to_binary(Field);
to_binary(Field) when is_float(Field) ->
  float_to_binary(Field);
to_binary(Field) when is_atom(Field) ->
  atom_to_binary(Field);
to_binary(Field) ->
  unicode:characters_to_binary(
    io_lib:format("~ts", [Field])).

% iolist_join: like lists:join but for iolist()
iolist_join([One], _, EndFlag) ->
  [One, EndFlag];
iolist_join([H | T], Sep, EndFlag) ->
  [H | join_rest(T, Sep, EndFlag)].

join_rest([], _, EndFlag) ->
  [EndFlag];
join_rest([H | T], Sep, EndFlag) ->
  [Sep, H | join_rest(T, Sep, EndFlag)].

%join_lines(Lines, LineEnd) ->
%  [Line || Line <- join_rest(Lines, LineEnd)] ++ LineEnd.
