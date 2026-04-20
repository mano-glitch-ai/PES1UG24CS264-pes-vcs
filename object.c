// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}
void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    SHA256((const unsigned char *)data, len, id_out->hash);

}


void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: Figure out the type string
    const char *type_str;
    if (type == OBJ_BLOB)        type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    // Step 2: Build the header e.g. "blob 16\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    // Step 3: Build the full object = header + data
    size_t full_len = header_len + len;
    uint8_t *full = malloc(full_len);
    if (!full) return -1;
    memcpy(full, header, header_len);
    memcpy(full + header_len, data, len);

    // Step 4: Hash the full object
    compute_hash(full, full_len, id_out);

    // Step 5: If it already exists, no need to store again
    if (object_exists(id_out)) {
        free(full);
        return 0;
    }

    // Step 6: Get the hex string of the hash
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    // Step 7: Create the shard directory .pes/objects/XX/
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(dir, 0755);

    // Step 8: Build the full path .pes/objects/XX/YYYY...
    char path[512];
    snprintf(path, sizeof(path), "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);

    // Step 9: Write to a temp file
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) { free(full); return -1; }

    if (write(fd, full, full_len) != (ssize_t)full_len) {
        close(fd); free(full); return -1;
    }
    fsync(fd);
    close(fd);
    free(full);

    // Step 10: Rename temp → final (atomic)
    if (rename(tmp_path, path) != 0) return -1;

    // Step 11: fsync the directory
    int dir_fd = open(dir, O_RDONLY);
    if (dir_fd >= 0) { fsync(dir_fd); close(dir_fd); }

    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Get the file path for this hash
    char path[512];
    object_path(id, path, sizeof(path));

    // Step 2: Open and read the whole file
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    size_t full_len = ftell(f);
    fseek(f, 0, SEEK_SET);

    uint8_t *full = malloc(full_len);
    if (!full) { fclose(f); return -1; }
    if (fread(full, 1, full_len, f) != full_len) {
        fclose(f); free(full); return -1;
    }
    fclose(f);

    // Step 3: Verify integrity — recompute hash and compare
    ObjectID computed;
    compute_hash(full, full_len, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(full); return -1;
    }

    // Step 4: Find the \0 that separates header from data
    uint8_t *null_ptr = memchr(full, '\0', full_len);
    if (!null_ptr) { free(full); return -1; }

    // Step 5: Parse the type from the header
    if (strncmp((char*)full, "blob", 4) == 0)        *type_out = OBJ_BLOB;
    else if (strncmp((char*)full, "tree", 4) == 0)   *type_out = OBJ_TREE;
    else if (strncmp((char*)full, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else { free(full); return -1; }

    // Step 6: Extract the data portion (everything after the \0)
    size_t header_len = (null_ptr - full) + 1;
    *len_out = full_len - header_len;
    *data_out = malloc(*len_out);
    if (!*data_out) { free(full); return -1; }
    memcpy(*data_out, full + header_len, *len_out);

    free(full);
    return 0;
}
