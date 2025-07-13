
// csv_nif.c — 流式 CSV NIF，基于 libcsv

#include "erl_nif.h"
#include "csv.h"
#include <string.h>
#include <stdlib.h>



typedef struct {
  char** fields;
  size_t count;
  size_t capacity;
  int to_term;
} row_data;

typedef struct {
  row_data* tmp_row_data;
  row_data* rows;
  size_t count;
  size_t capacity;
  int to_term;
}rows_list;

typedef struct {
  rows_list rl;
  ErlNifEnv* env;
} user_data;

// Resource structure
typedef struct {
    struct csv_parser parser;
    user_data ud;
    int finished;
} csv_nif_resource;

typedef struct{
  ErlNifResourceType* CSV_RES_TYPE;
  //user_data ud;
}priv_data;

void init_row_data(row_data* rd);
void add_fields(row_data* rd, void* data, size_t len);
void free_rd(row_data* rd);
ERL_NIF_TERM make_erl_row_data(ErlNifEnv* env, row_data* rd);
void init_rows_list(rows_list* l);
void add_rows(rows_list* l);
ERL_NIF_TERM to_erl_rows_list(ErlNifEnv*env, rows_list *l);
void free_rows_list(rows_list* l);

void field_cb(void *data, size_t len, void *user_da);
void row_cb(int c, void* user_da);

static void csv_dtor(ErlNifEnv* env, void* obj);

//static void unload(ErlNifEnv* env, void* priv_da) {
void 
init_row_data(row_data* rd) {
  rd->count = 0;
  rd->capacity = 128;
  rd->fields = (char**)malloc(sizeof(char*) * (rd->capacity));
  rd->to_term = 0;
}

void
add_fields(row_data* rd, void* data, size_t len){
  size_t count = rd->count;
  size_t capacity = rd->capacity;
  if (count >= capacity) {
    char** new_fields = realloc(rd->fields, sizeof(char*) * capacity*2);
    if (!new_fields){
      free(rd->fields);
      exit(1);
    }
    rd->fields = new_fields;
    rd->capacity = capacity*2;
  }
  rd->fields[count] = (char*)malloc(sizeof(char)*len + 1);
  memcpy((void*)rd->fields[count], data, len);
  rd->fields[count][len] = '\0';
  rd->count = rd->count + 1;
}

void
free_rd(row_data* rd){
  size_t count = rd->count;
  char** fields = rd->fields;
  if (!rd->to_term)
  {
    int i;
    for (i=0; i < count; i++) {
      char *bin =  fields[i];
      free(bin);
    }
  }
  free(fields);
  rd->fields = NULL;
}


ERL_NIF_TERM
make_erl_row_data(ErlNifEnv* env, row_data* rd){
  size_t count = rd->count;
  char** fields = rd->fields;
  int i;
  ERL_NIF_TERM* erl_lists = malloc(sizeof(ERL_NIF_TERM) * count);
  for (i=0;i<count;i++) {
    char* field = fields[i];
    unsigned char* data = enif_make_new_binary(env, strlen(field), &(erl_lists[i]));
    memcpy(data, field, strlen(field));
    free(field);
  }
  rd->to_term = 1;
  ERL_NIF_TERM row_list = enif_make_list_from_array(env, erl_lists, count);
  free(erl_lists);
  rd->count = 0;
  free_rd(rd);
  return row_list;
}



void
init_rows_list(rows_list* l){
  l->count =0;
  l->capacity = 128;
  l->rows =  (row_data*)malloc(sizeof(row_data) * (l->capacity));
  l->tmp_row_data = malloc(sizeof(row_data));
  init_row_data(l->tmp_row_data);
  //l->terms =  (ERL_NIF_TERM*)malloc(sizeof(ERL_NIF_TERM) * (l->capacity));
  l->to_term = 0;
}

void
add_rows(rows_list* l){
  row_data* tmp_rd = l->tmp_row_data;
  size_t count = l->count;
  size_t capacity = l->capacity;
  if (count >= capacity) {
    row_data* new_terms = realloc(l->rows, sizeof(row_data) * capacity*2);
    if (!new_terms){
      free(l->rows);
      exit(1);
    }
    l->rows= new_terms;
    l->capacity = capacity*2;
  }
  l->rows[count] = *tmp_rd;
  free(tmp_rd);
  l->count = l->count + 1;


  row_data* new_tmp_row = malloc(sizeof(row_data));
  init_row_data(new_tmp_row);
  l->tmp_row_data = new_tmp_row;
}


ERL_NIF_TERM
to_erl_rows_list(ErlNifEnv*env, rows_list *l)
{
  size_t count = l->count;
  row_data* rows = l->rows;
  int i=0;
  ERL_NIF_TERM* erl_lists = malloc(sizeof(ERL_NIF_TERM) * count);

  for(i=0; i<count; i++){
    row_data* row_d = &(rows[i]);
    ERL_NIF_TERM row_term = make_erl_row_data(env, row_d);
    erl_lists[i] = row_term;
  }
  ERL_NIF_TERM row_list = enif_make_list_from_array(env, erl_lists, count);
  
  l->to_term = 1;
  l->count = 0;
  return row_list;
}

void
free_rows_list(rows_list* l){
  free_rd(l->tmp_row_data);
  free(l->tmp_row_data);
  free(l->rows);
}


// Field callback: collect fields to a list (user_data is ErlNifEnv*)
void field_cb(void *data, size_t len, void *user_da) {
    // 可扩展：你可以将 field 存入某个列表等
  user_data* ud = (user_data*)user_da;
  row_data* rd = ud->rl.tmp_row_data;
  add_fields(rd, data, len);
}

// Row callback: row ends (可扩展)
void row_cb(int c, void* user_da) {
    // 可扩展：你可以标记一行结束
  user_data* ud = (user_data*)user_da;
  add_rows(&(ud->rl));
}

static int load(ErlNifEnv* env, void** priv_da, ERL_NIF_TERM load_info) {
  ErlNifResourceType *CSV_RES_TYPE = enif_open_resource_type(
      env, NULL, "user_data", csv_dtor,
      ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

  if (CSV_RES_TYPE == NULL) return -1;
  priv_data* pd = (priv_data*)enif_alloc(sizeof(priv_data));
  pd->CSV_RES_TYPE = CSV_RES_TYPE;
  *priv_da = pd;
  return 0;
}


// NIF: new/0
static ERL_NIF_TERM csv_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  priv_data* pd = enif_priv_data(env);
  csv_nif_resource* res = enif_alloc_resource(pd->CSV_RES_TYPE, sizeof(csv_nif_resource));
    if (res == NULL)
        return enif_make_badarg(env);
    if (csv_init(&(res->parser), CSV_STRICT) != 0) {
        enif_release_resource(res);
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                      enif_make_atom(env, "init_failed"));
    }
  res->finished = 0;
  init_rows_list(&(res->ud.rl));
  res->ud.env = env;
  ERL_NIF_TERM handle = enif_make_resource(env, res);
  enif_release_resource(res);
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), handle);
}

// NIF: parse_chunk(Handle, Binary)
static ERL_NIF_TERM parse_chunk(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  csv_nif_resource* res=NULL;
  ErlNifBinary bin;
  priv_data* pd = enif_priv_data(env);

  if (!enif_get_resource(env, argv[0], pd->CSV_RES_TYPE, (void**)&res))
      return enif_make_badarg(env);
  if (!enif_inspect_binary(env, argv[1], &bin))
      return enif_make_badarg(env);

  if (res->finished)
      return enif_make_atom(env, "already_finished");
  res->ud.env = env;
  size_t parsed = csv_parse(&(res->parser), bin.data, bin.size, field_cb, row_cb, &(res->ud));
  if (parsed != bin.size)
      return enif_make_tuple2(env, enif_make_atom(env, "error"),
                                    enif_make_string(env, csv_strerror(csv_error(&res->parser)), ERL_NIF_LATIN1));
  
  //return 
  user_data *ud = &(res->ud);
  rows_list *rl = &(ud->rl);
  ERL_NIF_TERM el = to_erl_rows_list(env, rl);
  return enif_make_tuple(env, 2, enif_make_atom(env, "ok"), el);
}

// NIF: close(Handle)
static ERL_NIF_TERM csv_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  priv_data* pd = enif_priv_data(env);
  csv_nif_resource* res;
  if (!enif_get_resource(env, argv[0], pd->CSV_RES_TYPE, (void**)&res))
      return enif_make_badarg(env);

  res->ud.env = env;
  if (!res->finished) {
      csv_free(&res->parser);
      res->finished = 1;
  }

  return enif_make_atom(env, "ok");
}
static ERL_NIF_TERM fini(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) 
{
  priv_data* pd = enif_priv_data(env);
  csv_nif_resource* res=NULL;
  if (!enif_get_resource(env, argv[0], pd->CSV_RES_TYPE, (void**)&res))
     return enif_make_badarg(env);

  res->ud.env = env;
  if (csv_fini(&(res->parser), field_cb, row_cb, &(res->ud))){
    return enif_make_atom(env, "error");
  };

  user_data *ud = &(res->ud);
  rows_list *rl = &(ud->rl);
  ERL_NIF_TERM el = to_erl_rows_list(env, rl);
  ud->rl.count = 0;
  return enif_make_tuple(env, 2, enif_make_atom(env, "ok"), el);
}


// NIF unload cleanup
static void csv_dtor(ErlNifEnv* env, void* obj) {
  csv_nif_resource* res = (csv_nif_resource*)obj;
  
  if (!res->finished) {
    csv_free(&res->parser);
  }
  user_data* ud = &(res->ud);
  rows_list* rl = &(ud->rl);
  free_rows_list(rl);
}


static void unload(ErlNifEnv* env, void* priv_da) {
  priv_data* pd = (priv_data*)priv_da;
    // 如果有动态分配的成员，先释放它们
  enif_free(pd);  // 释放你在 load 里用 enif_alloc 分配的内存
}

static ErlNifFunc nif_funcs[] = {
  {"csv_new", 0, csv_new},
  {"csv_parse_chunk", 2, parse_chunk},
  {"csv_fini", 1, fini},
  {"csv_close", 1, csv_close}
};

ERL_NIF_INIT(csve, nif_funcs, load, NULL, NULL, unload)

