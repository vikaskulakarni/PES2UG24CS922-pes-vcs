
#include "pes.h"
#include "index.h"
#include "commit.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

static int mkdir_if_needed(const char *path) {
    if (mkdir(path, 0755) == 0) return 0;
    return errno == EEXIST ? 0 : -1;
}

void cmd_init(void) {
    if (mkdir_if_needed(PES_DIR) != 0 ||
        mkdir_if_needed(OBJECTS_DIR) != 0 ||
        mkdir_if_needed(".pes/refs") != 0 ||
        mkdir_if_needed(REFS_DIR) != 0) {
        fprintf(stderr, "error: failed to create .pes directory structure\n");
        return;
    }

    FILE *f = fopen(HEAD_FILE, "w");
    if (!f) {
        fprintf(stderr, "error: failed to write %s\n", HEAD_FILE);
        return;
    }
    fprintf(f, "ref: refs/heads/main\n");
    fclose(f);

    printf("Initialized empty PES repository in .pes\n");
}

void cmd_add(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: pes add <file>...\n");
        return;
    }

    Index index;
    if (index_load(&index) != 0) {
        fprintf(stderr, "error: failed to load index\n");
        return;
    }

    int ok = 1;
    for (int i = 2; i < argc; i++) {
        if (index_add(&index, argv[i]) != 0) {
            fprintf(stderr, "error: failed to add '%s'\n", argv[i]);
            ok = 0;
        }
    }

    if (ok) printf("Added %d file(s)\n", argc - 2);
}

void cmd_status(void) {
    Index index;
    if (index_load(&index) != 0) {
        fprintf(stderr, "error: failed to load index\n");
        return;
    }
    index_status(&index);
}

void cmd_commit(int argc, char *argv[]) {
    const char *message = NULL;
    for (int i = 2; i + 1 < argc; i++) {
        if (strcmp(argv[i], "-m") == 0) {
            message = argv[i + 1];
            break;
        }
    }

    if (!message) {
        fprintf(stderr, "error: commit requires a message (-m \"message\")\n");
        return;
    }

    ObjectID id;
    if (commit_create(message, &id) != 0) {
        fprintf(stderr, "error: failed to create commit\n");
        return;
    }

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    printf("Committed: %.12s... %s\n", hex, message);
}

static void print_commit(const ObjectID *id, const Commit *commit, void *ctx) {
    (void)ctx;
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    printf("commit %s\n", hex);
    printf("Author: %s\n", commit->author);
    printf("Date:   %llu\n\n", (unsigned long long)commit->timestamp);
    printf("    %s\n\n", commit->message);
}

void cmd_log(void) {
    if (commit_walk(print_commit, NULL) != 0) {
        fprintf(stderr, "No commits yet.\n");
    }
}

void branch_list(void) {
    DIR *dir = opendir(REFS_DIR);
    if (!dir) {
        fprintf(stderr, "error: failed to open %s\n", REFS_DIR);
        return;
    }

    char current[256] = "";
    FILE *head = fopen(HEAD_FILE, "r");
    if (head) {
        char line[512];
        if (fgets(line, sizeof(line), head) && strncmp(line, "ref: refs/heads/", 16) == 0) {
            line[strcspn(line, "\r\n")] = '\0';
            size_t len = strnlen(line + 16, sizeof(current) - 1);
            memcpy(current, line + 16, len);
            current[len] = '\0';
        }
        fclose(head);
    }

    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        printf("%c %s\n", strcmp(ent->d_name, current) == 0 ? '*' : ' ', ent->d_name);
    }
    closedir(dir);
}

int branch_create(const char *name) {
    if (!name || strchr(name, '/')) return -1;

    ObjectID head;
    if (head_read(&head) != 0) return -1;

    char path[512];
    snprintf(path, sizeof(path), "%s/%s", REFS_DIR, name);
    if (access(path, F_OK) == 0) return -1;

    FILE *f = fopen(path, "w");
    if (!f) return -1;

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&head, hex);
    fprintf(f, "%s\n", hex);
    fclose(f);
    return 0;
}

int branch_delete(const char *name) {
    if (!name || strchr(name, '/')) return -1;
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", REFS_DIR, name);
    return unlink(path);
}

int checkout(const char *target) {
    if (!target || strchr(target, '/')) return -1;

    char path[512];
    snprintf(path, sizeof(path), "%s/%s", REFS_DIR, target);
    if (access(path, F_OK) != 0) return -1;

    FILE *f = fopen(HEAD_FILE, "w");
    if (!f) return -1;
    fprintf(f, "ref: refs/heads/%s\n", target);
    fclose(f);
    return 0;
}

// ─── PROVIDED: Phase 5 Command Wrappers ─────────────────────────────────────

// Usage: 
//   pes branch          (lists branches)
//   pes branch <name>   (creates a branch)
//   pes branch -d <name>(deletes a branch)
void cmd_branch(int argc, char *argv[]) {
    if (argc == 2) {
        branch_list();
    } else if (argc == 3) {
        if (branch_create(argv[2]) == 0) {
            printf("Created branch '%s'\n", argv[2]);
        } else {
            fprintf(stderr, "error: failed to create branch '%s'\n", argv[2]);
        }
    } else if (argc == 4 && strcmp(argv[2], "-d") == 0) {
        if (branch_delete(argv[3]) == 0) {
            printf("Deleted branch '%s'\n", argv[3]);
        } else {
            fprintf(stderr, "error: failed to delete branch '%s'\n", argv[3]);
        }
    } else {
        fprintf(stderr, "Usage:\n  pes branch\n  pes branch <name>\n  pes branch -d <name>\n");
    }
}

// Usage: pes checkout <branch_or_commit>
void cmd_checkout(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: pes checkout <branch_or_commit>\n");
        return;
    }

    const char *target = argv[2];
    if (checkout(target) == 0) {
        printf("Switched to '%s'\n", target);
    } else {
        fprintf(stderr, "error: checkout failed. Do you have uncommitted changes?\n");
    }
}

// ─── PROVIDED: Command dispatch ─────────────────────────────────────────────

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: pes <command> [args]\n");
        fprintf(stderr, "\nCommands:\n");
        fprintf(stderr, "  init            Create a new PES repository\n");
        fprintf(stderr, "  add <file>...   Stage files for commit\n");
        fprintf(stderr, "  status          Show working directory status\n");
        fprintf(stderr, "  commit -m <msg> Create a commit from staged files\n");
        fprintf(stderr, "  log             Show commit history\n");
        fprintf(stderr, "  branch          List, create, or delete branches\n");
        fprintf(stderr, "  checkout <ref>  Switch branches or restore working tree\n");
        return 1;
    }

    const char *cmd = argv[1];

    if      (strcmp(cmd, "init") == 0)     cmd_init();
    else if (strcmp(cmd, "add") == 0)      cmd_add(argc, argv);
    else if (strcmp(cmd, "status") == 0)   cmd_status();
    else if (strcmp(cmd, "commit") == 0)   cmd_commit(argc, argv);
    else if (strcmp(cmd, "log") == 0)      cmd_log();
    else if (strcmp(cmd, "branch") == 0)   cmd_branch(argc, argv);
    else if (strcmp(cmd, "checkout") == 0) cmd_checkout(argc, argv);
    else {
        fprintf(stderr, "Unknown command: %s\n", cmd);
        fprintf(stderr, "Run 'pes' with no arguments for usage.\n");
        return 1;
    }

    return 0;
}
