// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions: object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/sha.h>

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
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(id_out->hash, &ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
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

// ─── IMPLEMENTATION ──────────────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
// where <type> is "blob", "tree", or "commit"
// and <size> is the decimal string of the data length
//
// Steps:
//  1. Build the full object: header ("blob 16\0") + data
//  2. Compute SHA-256 hash of the FULL object (header + data)
//  3. Check if object already exists (deduplication) — if so, just return success
//  4. Create shard directory (.pes/objects/XX/) if it doesn't exist
//  5. Write to a temporary file in the same shard directory
//  6. fsync() the temporary file to ensure data reaches disk
//  7. rename() the temp file to the final path (atomic on POSIX)
//  8. Open and fsync() the shard directory to persist the rename
//  9. Store the computed hash in *id_out
//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: Determine type string
    const char *type_str;
    switch (type) {
        case OBJ_BLOB:   type_str = "blob";   break;
        case OBJ_TREE:   type_str = "tree";   break;
        case OBJ_COMMIT: type_str = "commit"; break;
        default: return -1;
    }

    // Build the header: "<type> <size>\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;
    // +1 because snprintf doesn't count the null terminator, but we need it included

    // Allocate buffer for the full object (header + data)
    size_t full_len = (size_t)header_len + len;
    uint8_t *full_obj = malloc(full_len);
    if (!full_obj) return -1;

    // Copy header (including the null terminator) and data
    memcpy(full_obj, header, (size_t)header_len);
    memcpy(full_obj + header_len, data, len);

    // Step 2: Compute SHA-256 hash of the full object
    compute_hash(full_obj, full_len, id_out);

    // Step 3: Deduplication — if object already exists, we're done
    if (object_exists(id_out)) {
        free(full_obj);
        return 0;
    }

    // Build paths
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    // Step 4: Create shard directory .pes/objects/XX/
    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755); // Ignore EEXIST — it's fine if it already exists

    // Final object path
    char final_path[512];
    object_path(id_out, final_path, sizeof(final_path));

    // Temp file path in same directory (same filesystem = atomic rename)
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%d", final_path, getpid());

    // Step 5: Write to temp file
    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0444);
    if (fd < 0) {
        free(full_obj);
        return -1;
    }

    ssize_t written = write(fd, full_obj, full_len);
    free(full_obj);

    if (written < 0 || (size_t)written != full_len) {
        close(fd);
        unlink(tmp_path);
        return -1;
    }

    // Step 6: fsync the temp file
    if (fsync(fd) != 0) {
        close(fd);
        unlink(tmp_path);
        return -1;
    }
    close(fd);

    // Step 7: Atomically rename temp file to final path
    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        return -1;
    }

    // Step 8: fsync the shard directory to persist the directory entry
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    return 0;
}

// Read an object from the store.
//
// Steps:
//  1. Build the file path from the hash using object_path()
//  2. Open and read the entire file
//  3. Parse the header to extract the type string and size
//  4. Verify integrity: recompute the SHA-256 of the file contents
//     and compare to the expected hash (from *id). Return -1 if mismatch.
//  5. Set *type_out to the parsed ObjectType
//  6. Allocate a buffer, copy the data portion (after the \0), set *data_out and *len_out
//
// The caller is responsible for calling free(*data_out).
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Get the file path
    char path[512];
    object_path(id, path, sizeof(path));

    // Step 2: Open and read the entire file into memory
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    // Get file size
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    long file_size = ftell(f);
    if (file_size < 0) { fclose(f); return -1; }
    rewind(f);

    uint8_t *buf = malloc((size_t)file_size);
    if (!buf) { fclose(f); return -1; }

    if (fread(buf, 1, (size_t)file_size, f) != (size_t)file_size) {
        free(buf);
        fclose(f);
        return -1;
    }
    fclose(f);

    // Step 4: Verify integrity — recompute hash and compare to expected
    ObjectID computed;
    compute_hash(buf, (size_t)file_size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        // Hash mismatch = corruption
        free(buf);
        return -1;
    }

    // Step 3: Parse the header: find the null terminator separating header from data
    uint8_t *null_pos = memchr(buf, '\0', (size_t)file_size);
    if (!null_pos) {
        free(buf);
        return -1;
    }

    // Extract type string from header (before the space)
    char *space_pos = memchr(buf, ' ', null_pos - buf);
    if (!space_pos) {
        free(buf);
        return -1;
    }

    // Parse object type
    size_t type_len = (size_t)(space_pos - (char *)buf);
    if (strncmp((char *)buf, "blob", type_len) == 0 && type_len == 4)
        *type_out = OBJ_BLOB;
    else if (strncmp((char *)buf, "tree", type_len) == 0 && type_len == 4)
        *type_out = OBJ_TREE;
    else if (strncmp((char *)buf, "commit", type_len) == 0 && type_len == 6)
        *type_out = OBJ_COMMIT;
    else {
        free(buf);
        return -1;
    }

    // Step 6: Copy the data portion (everything after the null terminator)
    size_t header_total = (size_t)(null_pos - buf) + 1; // header + null byte
    size_t data_len = (size_t)file_size - header_total;

    uint8_t *data_copy = malloc(data_len + 1); // +1 for safety null terminator
    if (!data_copy) {
        free(buf);
        return -1;
    }

    memcpy(data_copy, null_pos + 1, data_len);
    data_copy[data_len] = '\0'; // null terminate for convenience (text objects)

    free(buf);

    *data_out = data_copy;
    *len_out = data_len;
    return 0;
}
