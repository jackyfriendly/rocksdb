//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "rocksdb/c.h"

#include <unistd.h>  // sysconf() - get CPU count

const char DBPath[] = "/tmp/rocksdb_simple_example";
const char DBBackupPath[] = "/tmp/rocksdb_simple_example_backup";
const char CF_PREFIX[] = "cd_";

static all_slot_count = 16384;

rocksdb_options_t *opt;
rocksdb_readoptions_t *ropt;
rocksdb_writeoptions_t *wopt;
rocksdb_t *db;


static void rocksdb_option_init() {

    opt  = rocksdb_options_create();
    ropt = rocksdb_readoptions_create();
    wopt = rocksdb_writeoptions_create();

    rocksdb_options_set_create_if_missing(opt, 1);

    rocksdb_readoptions_set_verify_checksums(ropt, 1);
    rocksdb_readoptions_set_fill_cache(ropt, 1);

    rocksdb_writeoptions_set_sync(wopt, 1);
}

static int rocksdb_open_cf(const char *db_path, char **cf_names, int size)
{
    int i;
    char *err = NULL;

    const rocksdb_options_t **opts = malloc(sizeof(rocksdb_options_t*) * size);
    rocksdb_column_family_handle_t **handles = malloc(sizeof(rocksdb_column_family_handle_t*) * size);

    for (i=0; i<size; i++) {
        opts[i] = opt;
    }

    db = rocksdb_open_column_families(opt, db_path,
            size, (const char **)cf_names, opts, handles, &err);
    if (err != NULL) {
        fprintf(stderr, "file: %s, line: %d db path: %s, err: %s\n",
                __FILE__, __LINE__, db_path, err);
    } else {
        // handles[0] is default column family
        // cf_infos[max] is default column family handle
        fprintf(stderr, "file: %s, line: %d open column families success", __FILE__, __LINE__);
    }

    free(err);
    free(opts);
    free(handles);

    rocksdb_list_column_families_destroy(cf_names, size);
    return err == NULL ? 0 : -1;
}

int rocksdb_batch_add_cf(int slot_from, int slot_end)
{
    int i;
    int ret = 0;
    int op_cf_count;
    char** new_cf_names;
    char* err;
    int success_count = 0;
    rocksdb_column_family_handle_t** handles = NULL;
    char slot[128];

    op_cf_count = slot_end - slot_from + 1;

    new_cf_names = malloc(op_cf_count * sizeof(char*));
    for (i = slot_from; i <= slot_end; i++) {
        sprintf(slot, "%s%d", CF_PREFIX, i);
        new_cf_names[i] = strdup(slot);
    }
    ret = rocksdb_create_column_families(db, opt,
            (const char**)new_cf_names, op_cf_count, &handles, &success_count, &err);
    if (ret) {
        fprintf(stderr, "file: %s, line: %d create column families failed, fatal error: %s\n",
                __FILE__, __LINE__, err);
        free(err);
    } else {
        fprintf(stderr, "file: %s, line: %d batch create slots success count: %d\n",
                __FILE__, __LINE__, success_count);
    }
    for (i = 0; i <= slot_end; i++) {
        free(new_cf_names[i]);
    }
    free(new_cf_names);
    return ret;
}
int rocks_db_open(const char *path) {
    char *err = NULL;
    size_t size;
    int ret;
    
    char** cfs = rocksdb_list_column_families(opt, path, &size, &err);

    if (err != NULL) {
        fprintf(stderr,"file: %s, line: %d db path: %s,"
                " get column family empty, error: %s\n",
                __FILE__, __LINE__, path, err);

        free(err);
        cfs = malloc(sizeof(char*));
        cfs[0] = strdup("default");
        size = 1;
        ret = rocksdb_open_cf(path, cfs, 1);
        if (ret) {
            fprintf(stderr, "file: %s, line: %d column family open failed\n",
                    __FILE__, __LINE__);
            return ret;
        }
        return rocksdb_batch_add_cf(0, all_slot_count-1);
    }
    fprintf(stderr, "file: %s, line: %d column family size: %d\n",
            __FILE__, __LINE__, (int)size);
    return rocksdb_open_cf(path, cfs, size);
}

int main(int argc, char **argv) {
    int ret;
    
    rocksdb_option_init();
    ret = rocks_db_open(DBPath);
    if (ret) {
        fprintf(stderr, "db open failed\n");
        return ret;
    }
    while(1) {
        sleep(10);
    }
    return 0;
}
