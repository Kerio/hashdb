#include <stdio.h>
#include <stdlib.h>
#define __P(X) X

#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <db.h>

int compare(const DBT *key1, const DBT *key2) {
  char *ptr1, *ptr2;
   
  if (key1 == NULL) return 1;
  if (key2 == NULL) return -1;
  
  ptr1 = (char *)key1->data;
  ptr2 = (char *)key2->data;
  return strcmp(ptr1, ptr2);
}

int main(int argn, char *args[]) {
  DB *dbhandle;
  BTREEINFO bt;
  int limit;
  DBT key, data;
  int i,j,k;
  char *x, *y;

  /* Fill in the 'bt' thingy */
  bt.flags = 0;
  bt.cachesize = 0;
  bt.maxkeypage = 0;
  bt.minkeypage = 0;
  bt.psize = 0;
  bt.compare = compare;
  bt.prefix = NULL;
  bt.lorder = 0;
  
  /* A test: we create a database from parm 1, fill it with atoi(parm2)
     numbers, delete every other one, then query for randoms. */
  if (argn >= 3) { 
  dbhandle = dbopen(args[1], O_CREAT | O_RDWR, 0666, DB_BTREE, &bt);
  } else {
  dbhandle = dbopen(args[1], O_RDWR, 0666, DB_BTREE, &bt);
  } 

  if (!dbhandle) { 
    printf("Cannot open %s\n", args[1]);
    exit(1);
  }
 

  if (argn >= 3) {                
  limit = atoi(args[2]);
  printf("Testing to %d\n", limit);
  /* Now fill the database with the integers from 0 ... limit-1 */
  for (i=0;i<limit;i++) {
     key.data = malloc(64);
     sprintf((char *)key.data,"%d", i); 
    key.size = 64;
    data.data = malloc(64);
    sprintf((char *)data.data, "%d", limit-i);
    data.size = 64;
    if (key.data == NULL || data.data == NULL) {
      printf("Help ! One of key,data is null.\n");
    }
    /*printf("Adding %d %d\n", *((int *)key.data), *((int *)data.data));*/
    j = dbhandle->put(dbhandle, &key, &data, R_NOOVERWRITE);
    if (j) {
      printf("Couldn't put %d (%d)\n", i, j);
    }
    if (!(i%100)) printf("%d ", i);
 }
 printf("\nZapping every other record.\n");
  key.data = malloc(64);
  key.size = 64;
  for (i=0;i<(limit/2);i++) {
    sprintf((char *)key.data, "%d", 2*i);
    if (dbhandle->del(dbhandle, &key, 0)) {
      printf("Couldn't del %d\n", 2*i);
    }
    if (!(i%100)) printf("%d ", 2*i);
  }
    free(key.data);

  printf("\nDone\n");
  }  
k = 0;
 i = R_FIRST;
 do {

   j = dbhandle->seq(dbhandle, &key, &data, i);
   i = R_NEXT;
  

   if (!j) {
     if ((k%100) == 0) { printf("%s %s\n", 
				(char *)key.data, (char *)data.data); }
   } else {
     printf("Aargh ! Can't get data j %d. Errno = %d\n", j,errno);
   }
   k++; 
} while (j!=1);

  if (dbhandle->close(dbhandle)) {
    printf("Can't close\n");
  }
  
  return 0;
}

