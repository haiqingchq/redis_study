// 字符串 -> 字符串映射数据结构的大小已优化。
 
#ifndef _ZIPMAP_H
#define _ZIPMAP_H

unsigned char *zipmapNew(void);
unsigned char *zipmapSet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char *val, unsigned int vlen, int *update);
unsigned char *zipmapDel(unsigned char *zm, unsigned char *key, unsigned int klen, int *deleted);
unsigned char *zipmapRewind(unsigned char *zm);
unsigned char *zipmapNext(unsigned char *zm, unsigned char **key, unsigned int *klen, unsigned char **value, unsigned int *vlen);
int zipmapGet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char **value, unsigned int *vlen);
int zipmapExists(unsigned char *zm, unsigned char *key, unsigned int klen);
unsigned int zipmapLen(unsigned char *zm);
size_t zipmapBlobLen(unsigned char *zm);
void zipmapRepr(unsigned char *p);
int zipmapValidateIntegrity(unsigned char *zm, size_t size, int deep);

#ifdef REDIS_TEST
int zipmapTest(int argc, char *argv[], int flags);
#endif

#endif
