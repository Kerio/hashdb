#	@(#)Makefile	8.9 (Berkeley) 7/14/94

LIBDB=	libdb_d.lib
OBJ1=	hash.obj hash_bigkey.obj hash_buf.obj hash_func.obj hash_log2.obj hash_page.obj \
	hsearch.obj ndbm.obj
OBJ2=	bt_close.obj bt_conv.obj bt_debug.obj bt_delete.obj bt_get.obj bt_open.obj \
	bt_overflow.obj bt_page.obj bt_put.obj bt_search.obj bt_seq.obj bt_split.obj \
	bt_utils.obj
OBJ3=	db.obj
OBJ4=	mpool.obj
OBJ5=	rec_close.obj rec_delete.obj rec_get.obj rec_open.obj rec_put.obj rec_search.obj \
	rec_seq.obj rec_utils.obj
HDRS=   ../../btree/btree.h ../../btree/extern.h \
	../../hash/extern.h ../../hash/hash.h ../../hash/page.h ../../hash/search.h \
	../../include/db.h ../../include/mpool.h \
	../win32/include/cdefs.h ../win32/include/compat.h ../win32/include/db.h ../win32/include/getopt.h ../win32/include/mpool.h ../win32/include/my_getopt.h ../win32/include/ndbm.h ../win32/include/queue.h \
	../win32/include/sys/bdb_cdefs.h ../win32/include/sys/bdb_file.h ../win32/include/sys/mman.h ../win32/include/sys/param.h ../win32/include/unistd.h

DEBUG_FLAGS=-DDEBUG
#DEBUG_FLAGS=
#LIB_TYPE=-MT
LIB_TYPE=-MTd
#STAT=-DSTATISTICS
COMMON_CFLAGS=-DKERIO_WIN32 $(DEBUG_FLAGS) $(LIB_TYPE)
CFLAGS=$(COMMON_CFLAGS) -DKERIO_WIN_BLKSIZE="1024" $(STAT)
MISC=   mktemp.obj

$(LIBDB): $(OBJ1) $(OBJ2) $(OBJ3) $(OBJ4) $(OBJ5) $(MISC)
	link /lib /OUT:$@\
	     $(OBJ1) $(OBJ2) $(OBJ3) $(OBJ4) $(OBJ5) $(MISC)

clean:
	rm -f $(LIBDB) $(OBJ1) $(OBJ2) $(OBJ3) $(OBJ4) $(OBJ5) $(MISC) libdb.pdb

OORG=	-O2 -Zi /Fd"libdb_d.pdb"
CL=     $(CC) $(CFLAGS) -c -D__DBINTERFACE_PRIVATE $(OORG) -I. -Iinclude

hash.obj: ../../hash/hash.c $(HDRS)
	$(CL) -I../../hash ../../hash/hash.c
hash_bigkey.obj: ../../hash/hash_bigkey.c $(HDRS)
	$(CL) -I../../hash ../../hash/hash_bigkey.c
hash_buf.obj: ../../hash/hash_buf.c $(HDRS)
	$(CL) -I../../hash ../../hash/hash_buf.c
hash_func.obj: ../../hash/hash_func.c $(HDRS)
	$(CL) -I../../hash ../../hash/hash_func.c
hash_log2.obj: ../../hash/hash_log2.c $(HDRS)
	$(CL) -I../../hash ../../hash/hash_log2.c
hash_page.obj: ../../hash/hash_page.c $(HDRS)
	$(CL) -I../../hash ../../hash/hash_page.c
hsearch.obj: ../../hash/hsearch.c $(HDRS)
	$(CL) -I../../hash ../../hash/hsearch.c
ndbm.obj: ../../hash/ndbm.c $(HDRS)
	$(CL) -I../../hash ../../hash/ndbm.c

bt_close.obj: ../../btree/bt_close.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_close.c
bt_conv.obj: ../../btree/bt_conv.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_conv.c
bt_debug.obj: ../../btree/bt_debug.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_debug.c
bt_delete.obj: ../../btree/bt_delete.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_delete.c
bt_get.obj: ../../btree/bt_get.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_get.c
bt_open.obj: ../../btree/bt_open.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_open.c
bt_overflow.obj: ../../btree/bt_overflow.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_overflow.c
bt_page.obj: ../../btree/bt_page.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_page.c
bt_put.obj: ../../btree/bt_put.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_put.c
bt_search.obj: ../../btree/bt_search.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_search.c
bt_seq.obj: ../../btree/bt_seq.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_seq.c
bt_split.obj: ../../btree/bt_split.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_split.c
bt_stack.obj: ../../btree/bt_stack.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_stack.c
bt_utils.obj: ../../btree/bt_utils.c $(HDRS)
	$(CL) -I../../btree ../../btree/bt_utils.c

db.obj: ../../db/db.c $(HDRS)
	$(CL) ../../db/db.c

mpool.obj: ../../mpool/mpool.c $(HDRS)
	$(CL) -I../../mpool ../../mpool/mpool.c

rec_close.obj: ../../recno/rec_close.c $(HDRS)
	$(CL) -I../../recno ../../recno/rec_close.c
rec_delete.obj: ../../recno/rec_delete.c $(HDRS)
	$(CL) -I../../recno ../../recno/rec_delete.c
rec_get.obj: ../../recno/rec_get.c $(HDRS)
	$(CL) -I../../recno ../../recno/rec_get.c
rec_open.obj: ../../recno/rec_open.c $(HDRS)
	$(CL) -I../../recno ../../recno/rec_open.c
rec_put.obj: ../../recno/rec_put.c $(HDRS)
	$(CL) -I../../recno ../../recno/rec_put.c
rec_search.obj: ../../recno/rec_search.c $(HDRS)
	$(CL) -I../../recno ../../recno/rec_search.c
rec_seq.obj: ../../recno/rec_seq.c $(HDRS)
	$(CL) -I../../recno ../../recno/rec_seq.c
rec_utils.obj: ../../recno/rec_utils.c $(HDRS)
	$(CL) -I../../recno ../../recno/rec_utils.c

mktemp.obj: 	../clib/mktemp.c $(HDRS)
	$(CC) $(COMMON_CFLAGS) $(OORG) -c -O -I. -Iinclude ../clib/mktemp.c
