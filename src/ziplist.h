/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com> 版权所有。
 * 版权所有 (c) 2009-2012，Redis 有限公司。
 * 保留所有权利。
 *
 * 允许以源代码和二进制形式进行再分发和使用，无论是否经过
 * 允许在满足以下条件的情况下，以源代码和二进制形式进行再分发和使用，无论是否进行修改：
 *
 * 重新分发源代码必须保留上述版权声明、
 * 本条件清单和以下免责声明。
 * 二进制形式的再分发必须复制上述版权声明、本条件清单和以下免责声明。
 * 二进制形式的再分发必须在与该软件一起提供的
 * 文档和/或随发行版提供的其他材料中复制上述版权声明、本条件列表和以下免责声明。
 * Redis 的名称及其贡献者的名称均不得用于
 * 在未经
 * 事先书面许可。
 *
 * 本软件由版权所有者和贡献者 “按原样 ”提供。
 * 任何明示或暗示的保证，包括但不限于
 * 本软件由版权所有者和贡献者 “按原样 ”提供。
 * 免责声明。在任何情况下，版权所有者或贡献者均不
 * 对任何直接、间接、附带、特殊、惩戒性或
 * 直接、间接、附带、特殊、惩戒性或间接损害赔偿（包括但不限于采购
 * 使用、数据或利润损失；或业务中断）负责。
 * 合同、严格责任或侵权行为）。
 * 合同、严格责任或侵权（包括疏忽或其他原因）
 * 因使用本软件而以任何方式造成的损害，即使已被告知发生此类损害的可能性。
 * 即使已被告知此类损害的可能性。
 */

#ifndef _ZIPLIST_H
#define _ZIPLIST_H

#define ZIPLIST_HEAD 0
#define ZIPLIST_TAIL 1

/* Each entry in the ziplist is either a string or an integer. */
typedef struct {
    /* When string is used, it is provided with the length (slen). */
    unsigned char *sval;
    unsigned int slen;
    /* When integer is used, 'sval' is NULL, and lval holds the value. */
    long long lval;
} ziplistEntry;

unsigned char *ziplistNew(void);
unsigned char *ziplistMerge(unsigned char **first, unsigned char **second);
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where);
unsigned char *ziplistIndex(unsigned char *zl, int index);
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p);
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p);
unsigned int ziplistGet(unsigned char *p, unsigned char **sval, unsigned int *slen, long long *lval);
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen);
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p);
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num);
unsigned char *ziplistReplace(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen);
unsigned int ziplistCompare(unsigned char *p, unsigned char *s, unsigned int slen);
unsigned char *ziplistFind(unsigned char *zl, unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip);
unsigned int ziplistLen(unsigned char *zl);
size_t ziplistBlobLen(unsigned char *zl);
void ziplistRepr(unsigned char *zl);
typedef int (*ziplistValidateEntryCB)(unsigned char* p, unsigned int head_count, void* userdata);
int ziplistValidateIntegrity(unsigned char *zl, size_t size, int deep,
                             ziplistValidateEntryCB entry_cb, void *cb_userdata);
void ziplistRandomPair(unsigned char *zl, unsigned long total_count, ziplistEntry *key, ziplistEntry *val);
void ziplistRandomPairs(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals);
unsigned int ziplistRandomPairsUnique(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals);
int ziplistSafeToAdd(unsigned char* zl, size_t add);

#ifdef REDIS_TEST
int ziplistTest(int argc, char *argv[], int flags);
#endif

#endif /* _ZIPLIST_H */
