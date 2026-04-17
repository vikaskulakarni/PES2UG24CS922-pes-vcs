// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"
//
// Example single entry (conceptual):
//   "100644 hello.txt\0" followed by 32 raw bytes of SHA-256

#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// Implemented in other phases. Weak index_load keeps the standalone tree
// unit test linkable when it only builds tree.o and object.o.
extern int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
extern int index_load(Index *index) __attribute__((weak));

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    if (!data && len > 0) return -1;
    if (!tree_out) return -1;

    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end) {
        if (tree_out->count >= MAX_TREE_ENTRIES) return -1;

        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1; // Malformed data

        // Parse mode into an isolated buffer
        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1; // Skip space

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1; // Malformed data

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0'; // Ensure null-terminated

        ptr = null_byte + 1; // Skip null byte

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1; 
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

// Helper for qsort to ensure consistent tree hashing
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
// Returns 0 on success, -1 on error.
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    if (!tree || !data_out || !len_out) return -1;
    if (tree->count < 0 || tree->count > MAX_TREE_ENTRIES) return -1;

    // Estimate max size: (6 bytes mode + 1 byte space + 256 bytes name + 1 byte null + 32 bytes hash) per entry
    size_t max_size = tree->count * 296;
    if (max_size == 0) max_size = 1;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    // Create a mutable copy to sort entries (Git requirement)
    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        if (entry->name[0] == '\0' || strchr(entry->name, '/')) {
            free(buffer);
            return -1;
        }
        
        // Write mode and name (%o writes octal correctly for Git standards)
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // +1 to step over the null terminator written by sprintf
        
        // Write binary hash
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
//
// HINTS - Useful functions and concepts for this phase:
//   - index_load      : load the staged files into memory
//   - strchr          : find the first '/' in a path to separate directories from files
//   - strncmp         : compare prefixes to group files belonging to the same subdirectory
//   - Recursion       : you will likely want to create a recursive helper function 
//                       (e.g., `write_tree_level(entries, count, depth)`) to handle nested dirs.
//   - tree_serialize  : convert your populated Tree struct into a binary buffer
//   - object_write    : save that binary buffer to the store as OBJ_TREE
//
// Returns 0 on success, -1 on error.
static int path_has_prefix(const char *path, const char *prefix) {
    size_t prefix_len = strlen(prefix);
    return strncmp(path, prefix, prefix_len) == 0;
}

static int tree_contains_name(const Tree *tree, const char *name) {
    for (int i = 0; i < tree->count; i++) {
        if (strcmp(tree->entries[i].name, name) == 0) return 1;
    }
    return 0;
}

static int tree_add_entry(Tree *tree, uint32_t mode, const char *name, size_t name_len,
                          const ObjectID *hash) {
    if (tree->count >= MAX_TREE_ENTRIES) return -1;
    if (name_len == 0 || name_len >= sizeof(tree->entries[0].name)) return -1;

    TreeEntry *entry = &tree->entries[tree->count];
    entry->mode = mode;
    entry->hash = *hash;
    memcpy(entry->name, name, name_len);
    entry->name[name_len] = '\0';
    tree->count++;
    return 0;
}

static int write_tree_level(const Index *index, const char *prefix, ObjectID *id_out) {
    Tree tree;
    tree.count = 0;
    size_t prefix_len = strlen(prefix);

    for (int i = 0; i < index->count; i++) {
        const IndexEntry *entry = &index->entries[i];
        if (!path_has_prefix(entry->path, prefix)) continue;

        const char *remaining = entry->path + prefix_len;
        if (remaining[0] == '\0') continue;

        const char *slash = strchr(remaining, '/');
        if (!slash) {
            if (tree_add_entry(&tree, entry->mode, remaining, strlen(remaining), &entry->hash) != 0) {
                return -1;
            }
            continue;
        }

        size_t dir_len = (size_t)(slash - remaining);
        char dirname[256];
        if (dir_len == 0 || dir_len >= sizeof(dirname)) return -1;
        memcpy(dirname, remaining, dir_len);
        dirname[dir_len] = '\0';

        if (tree_contains_name(&tree, dirname)) continue;

        char child_prefix[512];
        int n = snprintf(child_prefix, sizeof(child_prefix), "%s%.*s/",
                         prefix, (int)dir_len, remaining);
        if (n < 0 || (size_t)n >= sizeof(child_prefix)) return -1;

        ObjectID subtree_id;
        if (write_tree_level(index, child_prefix, &subtree_id) != 0) return -1;
        if (tree_add_entry(&tree, MODE_DIR, dirname, dir_len, &subtree_id) != 0) return -1;
    }

    void *data;
    size_t len;
    if (tree_serialize(&tree, &data, &len) != 0) return -1;

    int rc = object_write(OBJ_TREE, data, len, id_out);
    free(data);
    return rc;
}

int tree_from_index(ObjectID *id_out) {
    if (!id_out || !index_load) return -1;

    Index index;
    if (index_load(&index) != 0) return -1;

    return write_tree_level(&index, "", id_out);
}
