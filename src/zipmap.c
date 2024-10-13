/* 字符串 -> 字符串映射数据结构的大小已优化。
 * 该文件实现了一个将字符串映射到其他字符串的数据结构
 * 实现了一个 O(n) 查找数据结构，其设计非常节省内存。
 * 高效。
 *
 * Redis Hash 类型将此数据结构用于由少量元素组成的哈希值。
 * 一旦达到给定的元素数量，就会切换到哈希表。
 * 元素达到一定数量后，就会切换到哈希表。
 *
 * 鉴于 Redis 哈希表多次被用于表示由少量字段组成的对象，因此这是个很大的问题。
 * 鉴于很多时候 Redis 哈希表都是用来表示由少量字段组成的对象，这在内存使用方面是一个很大的优势。
 *
 * --------------------------------------------------------------------------
 *
 * 版权所有 (c) 2009 年至今，Redis 有限公司。
 * 保留所有权利。
 *
 * 根据 Redis Source Available License 2.0 (RSALv2) 或 Server Side Public License v1 (SSPLv1) 授权。
 * (RSALv2) 或服务器端公共许可证 v1 (SSPLv1)。
 */

/* zipmap 的内存布局，如 “foo” => “bar”, “hello” => “world”：
 *
 * <zmlen><len>“foo”<len><free>“bar”<len>“hello”<len><free>"world”
 *
 <zmlen> 是 1 个字节长度，用于保存 zipmap 的当前大小。
 * 当 zipmap 长度大于或等于 254 时，不使用此值。
 * 当 zipmap 长度大于或等于 254 时，不使用此值，需要遍历 zipmap 以找出长度。
 *
 <len> 是以下字符串（键或值）的长度。
 <len> 长度以单个值或 5 个字节值编码。
 * 如果第一个字节值（无符号 8 位值）介于 0 和
 * 253，则为单字节长度。如果是 254，那么接下来就是一个四字节的无符号
 * 整数（按主机字节排序）。255 的值用于
 * 表示哈希结束。
 *
 <free> 是字符串后未使用的空闲字节数。
 * 是字符串后未使用的空闲字节数。例如，如果 "foo”
 * 被设置为 “bar”，之后 “foo ”将被设置为 “hi”，那么它将有一个
 * 如果以后该值再次增大，甚至为了添加键/值，将有一个空闲字节可供使用。
 * 如果合适，还可以添加键/值对。
 *
 <free> 始终是一个无符号 8 位数，因为如果在更新操作后
 * 更新操作后，如果有超过几个空闲字节，zipmap 将被
 * 重新分配，以确保它尽可能小。
 *
 * 上述两个元素散列的最简洁表示法实际上是
 *
 * "\x02\x03foo\x03\x00bar\x05hello\x05\x00world\xff”
 *
 * 请注意，因为键和值的前缀长度都是 “objects”、
 * 查找将花费 O(N)，其中 N 是 zipmap 中元素的数量，而不是 O(N)。
 * 而不是表示 zipmap 所需的字节数。
 * 这大大降低了常数时间。
 */



#include <stdio.h>
#include <string.h>
#include "zmalloc.h"
#include "endianconv.h"

#define ZIPMAP_BIGLEN 254
#define ZIPMAP_END 255

/* The following defines the max value for the <free> field described in the
 * comments above, that is, the max number of trailing bytes in a value. */
#define ZIPMAP_VALUE_MAX_FREE 4

/* The following macro returns the number of bytes needed to encode the length
 * for the integer value _l, that is, 1 byte for lengths < ZIPMAP_BIGLEN and
 * 5 bytes for all the other lengths. */
#define ZIPMAP_LEN_BYTES(_l) (((_l) < ZIPMAP_BIGLEN) ? 1 : sizeof(unsigned int)+1)

/* Create a new empty zipmap. */
unsigned char *zipmapNew(void) {
    unsigned char *zm = zmalloc(2);

    zm[0] = 0; /* Length */
    zm[1] = ZIPMAP_END;
    return zm;
}

/* Decode the encoded length pointed by 'p' */
static unsigned int zipmapDecodeLength(unsigned char *p) {
    unsigned int len = *p;

    if (len < ZIPMAP_BIGLEN) return len;
    memcpy(&len,p+1,sizeof(unsigned int));
    memrev32ifbe(&len);
    return len;
}

static unsigned int zipmapGetEncodedLengthSize(unsigned char *p) {
    return (*p < ZIPMAP_BIGLEN) ? 1: 5;
}

/* Encode the length 'l' writing it in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. */
static unsigned int zipmapEncodeLength(unsigned char *p, unsigned int len) {
    if (p == NULL) {
        return ZIPMAP_LEN_BYTES(len);
    } else {
        if (len < ZIPMAP_BIGLEN) {
            p[0] = len;
            return 1;
        } else {
            p[0] = ZIPMAP_BIGLEN;
            memcpy(p+1,&len,sizeof(len));
            memrev32ifbe(p+1);
            return 1+sizeof(len);
        }
    }
}

/* Search for a matching key, returning a pointer to the entry inside the
 * zipmap. Returns NULL if the key is not found.
 *
 * If NULL is returned, and totlen is not NULL, it is set to the entire
 * size of the zipmap, so that the calling function will be able to
 * reallocate the original zipmap to make room for more entries. */
static unsigned char *zipmapLookupRaw(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned int *totlen) {
    unsigned char *p = zm+1, *k = NULL;
    unsigned int l,llen;

    while(*p != ZIPMAP_END) {
        unsigned char free;

        /* Match or skip the key */
        l = zipmapDecodeLength(p);
        llen = zipmapEncodeLength(NULL,l);
        if (key != NULL && k == NULL && l == klen && !memcmp(p+llen,key,l)) {
            /* Only return when the user doesn't care
             * for the total length of the zipmap. */
            if (totlen != NULL) {
                k = p;
            } else {
                return p;
            }
        }
        p += llen+l;
        /* Skip the value as well */
        l = zipmapDecodeLength(p);
        p += zipmapEncodeLength(NULL,l);
        free = p[0];
        p += l+1+free; /* +1 to skip the free byte */
    }
    if (totlen != NULL) *totlen = (unsigned int)(p-zm)+1;
    return k;
}

static unsigned long zipmapRequiredLength(unsigned int klen, unsigned int vlen) {
    unsigned int l;

    l = klen+vlen+3;
    if (klen >= ZIPMAP_BIGLEN) l += 4;
    if (vlen >= ZIPMAP_BIGLEN) l += 4;
    return l;
}

/* Return the total amount used by a key (encoded length + payload) */
static unsigned int zipmapRawKeyLength(unsigned char *p) {
    unsigned int l = zipmapDecodeLength(p);
    return zipmapEncodeLength(NULL,l) + l;
}

/* Return the total amount used by a value
 * (encoded length + single byte free count + payload) */
static unsigned int zipmapRawValueLength(unsigned char *p) {
    unsigned int l = zipmapDecodeLength(p);
    unsigned int used;

    used = zipmapEncodeLength(NULL,l);
    used += p[used] + 1 + l;
    return used;
}

/* If 'p' points to a key, this function returns the total amount of
 * bytes used to store this entry (entry = key + associated value + trailing
 * free space if any). */
static unsigned int zipmapRawEntryLength(unsigned char *p) {
    unsigned int l = zipmapRawKeyLength(p);
    return l + zipmapRawValueLength(p+l);
}

static inline unsigned char *zipmapResize(unsigned char *zm, unsigned int len) {
    zm = zrealloc(zm, len);
    zm[len-1] = ZIPMAP_END;
    return zm;
}

/* Set key to value, creating the key if it does not already exist.
 * If 'update' is not NULL, *update is set to 1 if the key was
 * already preset, otherwise to 0. */
unsigned char *zipmapSet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char *val, unsigned int vlen, int *update) {
    unsigned int zmlen, offset;
    unsigned int freelen, reqlen = zipmapRequiredLength(klen,vlen);
    unsigned int empty, vempty;
    unsigned char *p;

    freelen = reqlen;
    if (update) *update = 0;
    p = zipmapLookupRaw(zm,key,klen,&zmlen);
    if (p == NULL) {
        /* Key not found: enlarge */
        zm = zipmapResize(zm, zmlen+reqlen);
        p = zm+zmlen-1;
        zmlen = zmlen+reqlen;

        /* Increase zipmap length (this is an insert) */
        if (zm[0] < ZIPMAP_BIGLEN) zm[0]++;
    } else {
        /* Key found. Is there enough space for the new value? */
        /* Compute the total length: */
        if (update) *update = 1;
        freelen = zipmapRawEntryLength(p);
        if (freelen < reqlen) {
            /* Store the offset of this key within the current zipmap, so
             * it can be resized. Then, move the tail backwards so this
             * pair fits at the current position. */
            offset = p-zm;
            zm = zipmapResize(zm, zmlen-freelen+reqlen);
            p = zm+offset;

            /* The +1 in the number of bytes to be moved is caused by the
             * end-of-zipmap byte. Note: the *original* zmlen is used. */
            memmove(p+reqlen, p+freelen, zmlen-(offset+freelen+1));
            zmlen = zmlen-freelen+reqlen;
            freelen = reqlen;
        }
    }

    /* We now have a suitable block where the key/value entry can
     * be written. If there is too much free space, move the tail
     * of the zipmap a few bytes to the front and shrink the zipmap,
     * as we want zipmaps to be very space efficient. */
    empty = freelen-reqlen;
    if (empty >= ZIPMAP_VALUE_MAX_FREE) {
        /* First, move the tail <empty> bytes to the front, then resize
         * the zipmap to be <empty> bytes smaller. */
        offset = p-zm;
        memmove(p+reqlen, p+freelen, zmlen-(offset+freelen+1));
        zmlen -= empty;
        zm = zipmapResize(zm, zmlen);
        p = zm+offset;
        vempty = 0;
    } else {
        vempty = empty;
    }

    /* Just write the key + value and we are done. */
    /* Key: */
    p += zipmapEncodeLength(p,klen);
    memcpy(p,key,klen);
    p += klen;
    /* Value: */
    p += zipmapEncodeLength(p,vlen);
    *p++ = vempty;
    memcpy(p,val,vlen);
    return zm;
}

/* Remove the specified key. If 'deleted' is not NULL the pointed integer is
 * set to 0 if the key was not found, to 1 if it was found and deleted. */
unsigned char *zipmapDel(unsigned char *zm, unsigned char *key, unsigned int klen, int *deleted) {
    unsigned int zmlen, freelen;
    unsigned char *p = zipmapLookupRaw(zm,key,klen,&zmlen);
    if (p) {
        freelen = zipmapRawEntryLength(p);
        memmove(p, p+freelen, zmlen-((p-zm)+freelen+1));
        zm = zipmapResize(zm, zmlen-freelen);

        /* Decrease zipmap length */
        if (zm[0] < ZIPMAP_BIGLEN) zm[0]--;

        if (deleted) *deleted = 1;
    } else {
        if (deleted) *deleted = 0;
    }
    return zm;
}

/* Call before iterating through elements via zipmapNext() */
unsigned char *zipmapRewind(unsigned char *zm) {
    return zm+1;
}

/* This function is used to iterate through all the zipmap elements.
 * In the first call the first argument is the pointer to the zipmap + 1.
 * In the next calls what zipmapNext returns is used as first argument.
 * Example:
 *
 * unsigned char *i = zipmapRewind(my_zipmap);
 * while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
 *     printf("%d bytes key at $p\n", klen, key);
 *     printf("%d bytes value at $p\n", vlen, value);
 * }
 */
unsigned char *zipmapNext(unsigned char *zm, unsigned char **key, unsigned int *klen, unsigned char **value, unsigned int *vlen) {
    if (zm[0] == ZIPMAP_END) return NULL;
    if (key) {
        *key = zm;
        *klen = zipmapDecodeLength(zm);
        *key += ZIPMAP_LEN_BYTES(*klen);
    }
    zm += zipmapRawKeyLength(zm);
    if (value) {
        *value = zm+1;
        *vlen = zipmapDecodeLength(zm);
        *value += ZIPMAP_LEN_BYTES(*vlen);
    }
    zm += zipmapRawValueLength(zm);
    return zm;
}

/* Search a key and retrieve the pointer and len of the associated value.
 * If the key is found the function returns 1, otherwise 0. */
int zipmapGet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char **value, unsigned int *vlen) {
    unsigned char *p;

    if ((p = zipmapLookupRaw(zm,key,klen,NULL)) == NULL) return 0;
    p += zipmapRawKeyLength(p);
    *vlen = zipmapDecodeLength(p);
    *value = p + ZIPMAP_LEN_BYTES(*vlen) + 1;
    return 1;
}

/* Return 1 if the key exists, otherwise 0 is returned. */
int zipmapExists(unsigned char *zm, unsigned char *key, unsigned int klen) {
    return zipmapLookupRaw(zm,key,klen,NULL) != NULL;
}

/* Return the number of entries inside a zipmap */
unsigned int zipmapLen(unsigned char *zm) {
    unsigned int len = 0;
    if (zm[0] < ZIPMAP_BIGLEN) {
        len = zm[0];
    } else {
        unsigned char *p = zipmapRewind(zm);
        while((p = zipmapNext(p,NULL,NULL,NULL,NULL)) != NULL) len++;

        /* Re-store length if small enough */
        if (len < ZIPMAP_BIGLEN) zm[0] = len;
    }
    return len;
}

/* Return the raw size in bytes of a zipmap, so that we can serialize
 * the zipmap on disk (or everywhere is needed) just writing the returned
 * amount of bytes of the C array starting at the zipmap pointer. */
size_t zipmapBlobLen(unsigned char *zm) {
    unsigned int totlen;
    zipmapLookupRaw(zm,NULL,0,&totlen);
    return totlen;
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
int zipmapValidateIntegrity(unsigned char *zm, size_t size, int deep) {
#define OUT_OF_RANGE(p) ( \
        (p) < zm + 2 || \
        (p) > zm + size - 1)
    unsigned int l, s, e;

    /* check that we can actually read the header (or ZIPMAP_END). */
    if (size < 2)
        return 0;

    /* the last byte must be the terminator. */
    if (zm[size-1] != ZIPMAP_END)
        return 0;

    if (!deep)
        return 1;

    unsigned int count = 0;
    unsigned char *p = zm + 1; /* skip the count */
    while(*p != ZIPMAP_END) {
        /* read the field name length encoding type */
        s = zipmapGetEncodedLengthSize(p);
        /* make sure the entry length doesn't reach outside the edge of the zipmap */
        if (OUT_OF_RANGE(p+s))
            return 0;

        /* read the field name length */
        l = zipmapDecodeLength(p);
        p += s; /* skip the encoded field size */
        p += l; /* skip the field */

        /* make sure the entry doesn't reach outside the edge of the zipmap */
        if (OUT_OF_RANGE(p))
            return 0;

        /* read the value length encoding type */
        s = zipmapGetEncodedLengthSize(p);
        /* make sure the entry length doesn't reach outside the edge of the zipmap */
        if (OUT_OF_RANGE(p+s))
            return 0;

        /* read the value length */
        l = zipmapDecodeLength(p);
        p += s; /* skip the encoded value size*/
        e = *p++; /* skip the encoded free space (always encoded in one byte) */
        p += l+e; /* skip the value and free space */
        count++;

        /* make sure the entry doesn't reach outside the edge of the zipmap */
        if (OUT_OF_RANGE(p))
            return 0;
    }

    /* check that the zipmap is not empty. */
    if (count == 0) return 0;

    /* check that the count in the header is correct */
    if (zm[0] != ZIPMAP_BIGLEN && zm[0] != count)
        return 0;

    return 1;
#undef OUT_OF_RANGE
}

#ifdef REDIS_TEST
static void zipmapRepr(unsigned char *p) {
    unsigned int l;

    printf("{status %u}",*p++);
    while(1) {
        if (p[0] == ZIPMAP_END) {
            printf("{end}");
            break;
        } else {
            unsigned char e;

            l = zipmapDecodeLength(p);
            printf("{key %u}",l);
            p += zipmapEncodeLength(NULL,l);
            if (l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l;

            l = zipmapDecodeLength(p);
            printf("{value %u}",l);
            p += zipmapEncodeLength(NULL,l);
            e = *p++;
            if (l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l+e;
            if (e) {
                printf("[");
                while(e--) printf(".");
                printf("]");
            }
        }
    }
    printf("\n");
}

#define UNUSED(x) (void)(x)
int zipmapTest(int argc, char *argv[], int flags) {
    unsigned char *zm;

    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    zm = zipmapNew();

    zm = zipmapSet(zm,(unsigned char*) "name",4, (unsigned char*) "foo",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "surname",7, (unsigned char*) "foo",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "age",3, (unsigned char*) "foo",3,NULL);
    zipmapRepr(zm);

    zm = zipmapSet(zm,(unsigned char*) "hello",5, (unsigned char*) "world!",6,NULL);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "bar",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "!",1,NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "12345",5,NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm,(unsigned char*) "new",3, (unsigned char*) "xx",2,NULL);
    zm = zipmapSet(zm,(unsigned char*) "noval",5, (unsigned char*) "",0,NULL);
    zipmapRepr(zm);
    zm = zipmapDel(zm,(unsigned char*) "new",3,NULL);
    zipmapRepr(zm);

    printf("\nLook up large key:\n");
    {
        unsigned char buf[512];
        unsigned char *value;
        unsigned int vlen, i;
        for (i = 0; i < 512; i++) buf[i] = 'a';

        zm = zipmapSet(zm,buf,512,(unsigned char*) "long",4,NULL);
        if (zipmapGet(zm,buf,512,&value,&vlen)) {
            printf("  <long key> is associated to the %d bytes value: %.*s\n",
                vlen, vlen, value);
        }
    }

    printf("\nPerform a direct lookup:\n");
    {
        unsigned char *value;
        unsigned int vlen;

        if (zipmapGet(zm,(unsigned char*) "foo",3,&value,&vlen)) {
            printf("  foo is associated to the %d bytes value: %.*s\n",
                vlen, vlen, value);
        }
    }
    printf("\nIterate through elements:\n");
    {
        unsigned char *i = zipmapRewind(zm);
        unsigned char *key, *value;
        unsigned int klen, vlen;

        while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
            printf("  %d:%.*s => %d:%.*s\n", klen, klen, key, vlen, vlen, value);
        }
    }
    zfree(zm);
    return 0;
}
#endif
