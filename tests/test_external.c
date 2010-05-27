
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

int main(int argc, char **argv)
{
  const Pvoid_t params = read_parameters();
  test_param(params, "test1", "1,2,3");
  test_param(params, "one two three", "dim\ndam\n");
  test_param(params, "dummy", "value");
  msg("All parameters ok!");

  const char DVAL[] = "dkey";
  p_entry *dkey = dxmalloc(sizeof(p_entry) + sizeof(DVAL));
  dkey->len = sizeof(DVAL) - 1;
  memcpy(dkey->data, DVAL, dkey->len);

  int i = 0;
  p_entry *key = NULL;
  p_entry *val = NULL;
  while (read_kv(&key, &val)){
    msg("Got key <%s> val <%s>", key->data, val->data);
    if (!key->len)
      key = dkey;
    write_num_prefix(3);
    write_kv(key, val);
    write_kv(key, val);
    write_kv(key, val);
    ++i;
  }
  msg("%d key-value pairs read ok", i);
  return 0;
}
