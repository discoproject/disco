
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "disco.h"

void test_param(const Pvoid_t params, const char *param, const char *cmp)
{
  Word_t *ptr;
  JSLG(ptr, params, (unsigned char*)param);
  if (!ptr)
    die("parameter %s not found", param);
  if (strcmp(((p_entry*)*ptr)->data, cmp))
    die("parameter %s value invalid: Expected %s got %s",
        param, cmp, ((p_entry*)*ptr)->data);
}

void map(const Pvoid_t params)
{
  const char DVAL[] = "dkey";
  p_entry *dkey = dxmalloc(sizeof(p_entry) + sizeof(DVAL));
  dkey->len = sizeof(DVAL) - 1;
  memcpy(dkey->data, DVAL, dkey->len);

  int i = 0;
  p_entry *key = NULL;
  p_entry *val = NULL;
  while (read_kv(&key, &val)){
    msg("Got key <%.*s> val <%.*s>", key->len, key->data, val->len - 1, val->data);
    if (!key->len)
      key = dkey;
    write_num_prefix(3);
    write_kv(key, val);
    write_kv(key, val);
    write_kv(key, val);
    ++i;
  }
  msg("%d key-value pairs read ok", i);
}

void reduce(const Pvoid_t params)
{
  p_entry *res = dxmalloc(sizeof(p_entry) + 20);
  p_entry *key = NULL;
  p_entry *val = NULL;
  unsigned int i, sum = 0;
  while (read_kv(&key, &val)){
    msg("Got key <%.*s> val <%.*s>", key->len, key->data, val->len - 1, val->data);
    for (i = 0; i < val->len; i++)
        sum += val->data[i];
  }
  res->len = snprintf(res->data, 20, "%d", sum);
  write_num_prefix(10);
  for (i = 0; i < 10; i++)
      write_kv(res, res);
}

int main(int argc, char **argv)
{
  const Pvoid_t params = read_parameters();
  test_param(params, "test1", "1,2,3");
  test_param(params, "one two three", "dim\ndam\n");
  test_param(params, "dummy", "value");
  msg("All parameters ok!");

  if (!strcmp(argv[1], "map"))
      map(params);
  else
      reduce(params);
  return 0;
}
