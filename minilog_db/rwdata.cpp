#include "rwdata.h"
#include "BufferPool.h"
#include "WAL.h"
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <sys/types.h>
#include <string.h>
#include <string>

using namespace SQL;
// Helper function, extract pure table from filename (remove .bin)
// Since WAL::recover automatically adds .bin suffix, we must use the pure table name when writing logs
std::string getTableNameFromPath(const char* path) {
    std::string s(path);
    size_t lastindex = s.find_last_of("."); 
    if (lastindex == std::string::npos) return s;
    return s.substr(0, lastindex); 
}


FileManager* FileManager::getInstance() {
    static FileManager* m = new FileManager();
    return m;
}

// getCInternalNode using BufferPool
inter_node FileManager::getCInternalNode(Index idx, void* data[MAXNUM_KEY], off_t offt) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(idx.fpath, offt);
    
    if (!page) {
        inter_node empty_node;
        memset(&empty_node, 0, sizeof(inter_node));
        return empty_node;
    }
    
    // Read node data from page buffer
    inter_node node;
    memcpy(&node, page->data, sizeof(inter_node));
    
    // Read key data (following the node structure)
    off_t arr_offt = sizeof(inter_node);
    idx.offt_self = arr_offt;
    
    // Read key values from buffer page
    if (idx.key_kind == INT_KEY) {
        int* temp = (int*)(page->data + arr_offt);
        for (int i = 0; i < MAXNUM_KEY; i++) {
            data[i] = new int(temp[i]);
        }
    } else if (idx.key_kind == LL_KEY) {
        long long* temp = (long long*)(page->data + arr_offt);
        for (int i = 0; i < MAXNUM_KEY; i++) {
            data[i] = new long long(temp[i]);
        }
    } else {
        for (int i = 0; i < MAXNUM_KEY; i++) {
            char* temp = new char[1024];
            memcpy(temp, page->data + arr_offt + i * idx.max_size, idx.max_size);
            data[i] = temp;
        }
    }
    
    // Release page (decrement pin count)
    bp->unpinPage(idx.fpath, offt, false);
    
    return node;
}

// flushInterNode
bool FileManager::flushInterNode(inter_node node, Index idx, void** key) {
    BufferPool* bp = BufferPool::getInstance();
    off_t block_idx = idx.offt_self / DB_BLOCK_SIZE;
    Page* page = bp->getPage(idx.fpath, block_idx);
    
    if (!page) {
        return false;
    }
    
    // Write node structure
    memcpy(page->data, &node, sizeof(inter_node));
    
    // Write key data
    off_t offset = sizeof(inter_node);
    
    if (idx.key_kind == INT_KEY) {
        int temp[MAXNUM_KEY];
        for (int i = 0; i < MAXNUM_KEY; i++) {
            temp[i] = *(int*)key[i];
        }
        memcpy(page->data + offset, temp, sizeof(int) * MAXNUM_KEY);
    } else if (idx.key_kind == LL_KEY) {
        long long temp[MAXNUM_KEY];
        for (int i = 0; i < MAXNUM_KEY; i++) {
            temp[i] = *(long long*)key[i];
        }
        memcpy(page->data + offset, temp, sizeof(long long) * MAXNUM_KEY);
    } else {
        for (int i = 0; i < MAXNUM_KEY; i++) {
            memcpy(page->data + offset + i * idx.max_size, 
                   (char*)key[i], idx.max_size);
        }
    }

    // Write WAL log (Page Image)
    // Even for Update, we log complete Insert (overwrite) log for easier recovery via memcpy
    std::string table_name = getTableNameFromPath(idx.fpath);
    WAL::getInstance()->logInsert(table_name, block_idx, page->data, DB_BLOCK_SIZE);
    
    // Mark as dirty and release
    bp->unpinPage(idx.fpath, block_idx, true);
    
    return true;
}

// getLeafNode
leaf_node FileManager::getLeafNode(Index idx, void* data[MAXNUM_DATA], off_t offt) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(idx.fpath, offt);
    
    if (!page) {
        leaf_node empty_node;
        memset(&empty_node, 0, sizeof(leaf_node));
        return empty_node;
    }
    
    // Read node data from page buffer
    leaf_node node;
    memcpy(&node, page->data, sizeof(leaf_node));
    
    // Read data values (following the node structure)
    off_t arr_offt = sizeof(leaf_node);
    idx.offt_self = arr_offt;
    
    // Read data values from buffer page
    if (idx.key_kind == INT_KEY) {
        int* temp = (int*)(page->data + arr_offt);
        for (int i = 0; i < MAXNUM_DATA; i++) {
            data[i] = new int(temp[i]);
        }
    } else if (idx.key_kind == LL_KEY) {
        long long* temp = (long long*)(page->data + arr_offt);
        for (int i = 0; i < MAXNUM_DATA; i++) {
            data[i] = new long long(temp[i]);
        }
    } else {
        for (int i = 0; i < MAXNUM_DATA; i++) {
            char* temp = new char[1024];
            memcpy(temp, page->data + arr_offt + i * idx.max_size, idx.max_size);
            data[i] = temp;
        }
    }
    
    // Release page
    bp->unpinPage(idx.fpath, offt, false);
    
    return node;
}

// flushLeafNode
bool FileManager::flushLeafNode(leaf_node node, Index idx, void** value) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(idx.fpath, node.offt_self);
    
    if (!page) {
        return false;
    }
    
    // Write node structure
    memcpy(page->data, &node, sizeof(leaf_node));
    
    // Write data values
    off_t offset = sizeof(leaf_node);
    idx.offt_self = offset;
    
    if (idx.key_kind == INT_KEY) {
        int temp[MAXNUM_DATA];
        for (int i = 0; i < MAXNUM_DATA; i++) {
            temp[i] = *(int*)value[i];
        }
        memcpy(page->data + offset, temp, sizeof(int) * MAXNUM_DATA);
    } else if (idx.key_kind == LL_KEY) {
        long long temp[MAXNUM_DATA];
        for (int i = 0; i < MAXNUM_DATA; i++) {
            temp[i] = *(long long*)value[i];
        }
        memcpy(page->data + offset, temp, sizeof(long long) * MAXNUM_DATA);
    } else {
        for (int i = 0; i < MAXNUM_DATA; i++) {
            memcpy(page->data + offset + i * idx.max_size,
                   (char*)value[i], idx.max_size);
        }
    }

    // Write WAL log
    std::string table_name = getTableNameFromPath(idx.fpath);
    WAL::getInstance()->logInsert(table_name, node.offt_self, page->data, DB_BLOCK_SIZE);
    
    // Mark as dirty and release
    bp->unpinPage(idx.fpath, node.offt_self, true);
    
    return true;
}

// getTable
table FileManager::getTable(const char* filename, off_t offt) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(filename, offt);
    
    table t;
    if (page) {
        memcpy(&t, page->data, sizeof(table));
        bp->unpinPage(filename, offt, false);
    } else {
        memset(&t, 0, sizeof(table));
    }
    
    return t;
}

// flushTable
bool FileManager::flushTable(table t, const char* filename, off_t offt) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(filename, offt);
    
    if (!page) {
        return false;
    }
    
    memcpy(page->data, &t, sizeof(table));

    // Write WAL log and table header also needs recovery
    std::string table_name = getTableNameFromPath(filename);
    WAL::getInstance()->logInsert(table_name, offt, page->data, DB_BLOCK_SIZE);

    bp->unpinPage(filename, offt, true);
    
    return true;
}

void FileManager::flush_value(void* value[MAXNUM_DATA], Index idx) {
    FILE* file = fopen(idx.fpath, "rb+");
    if (fseek(file, idx.offt_self, SEEK_SET) != 0) {
        perror("Failed to seek");
        fclose(file);
    }

    if (idx.key_kind == INT_KEY) {
        int temp[MAXNUM_DATA];
        for (int i = 0; i < MAXNUM_DATA; i++) {
            temp[i] = *(int*)value[i];
        }
        fwrite(&temp, sizeof(int), MAXNUM_DATA, file);
    }
    else if (idx.key_kind == LL_KEY) {
        long long temp[MAXNUM_DATA];
        for (int i = 0; i < MAXNUM_DATA; i++) {
            temp[i] = *(long long*)value[i];
        }
        fwrite(&temp, sizeof(long long), MAXNUM_DATA, file);
    }
    else {
        for (int i = 0; i < MAXNUM_DATA; i++) {
            char* temp = (char*)value[i];
            fwrite(temp, sizeof(char), idx.max_size, file);
        }
    }
    fclose(file);
}

void FileManager::get_key(void* key[MAXNUM_KEY], Index idx) {
    FILE* file = fopen(idx.fpath, "rb");
    if (fseek(file, idx.offt_self, SEEK_SET) != 0) {
        perror("Failed to seek");
        fclose(file);
    }
    if (idx.key_kind == INT_KEY) {
        for (int i = 0; i < MAXNUM_DATA; ++i) {
            int *temp = new int();
            if (fread(temp, sizeof(int), 1, file) != 1) {
                perror("Failed to read data");
                fclose(file);
                return;
            }
            key[i] = (void*)temp;
        }
    }
    else if (idx.key_kind == LL_KEY) {
        for (int i = 0; i < MAXNUM_DATA; ++i) {
            long long *temp = new long long();
            if (fread(temp, sizeof(long long), 1, file) != 1) {
                perror("Failed to read data");
                fclose(file);
                return;
            }
            key[i] = (void*)temp;
        }
    }
    else {
        for (int i = 0; i < MAXNUM_DATA; ++i) {
            char temp[1024] = { 0 };
            if (fread(temp, sizeof(char), idx.max_size, file) != idx.max_size) {
                perror("Failed to read data");
                fclose(file);
                return;
            }
            key[i] = (void*)temp;
        }
    }
    fclose(file);
}

void FileManager::get_value(void* value[MAXNUM_DATA], Index idx) {
    FILE* file = fopen(idx.fpath, "rb");
    if (fseek(file, idx.offt_self, SEEK_SET) != 0) {
        perror("Failed to seek");
        fclose(file);
    }

    if (idx.key_kind == INT_KEY) {
        for (int i = 0; i < MAXNUM_DATA; ++i) {
            int *temp = new int();
            if (fread(temp, sizeof(int), 1, file) != 1) {
                perror("Failed to read data");
                fclose(file);
                return;
            }
            value[i] = (void*)temp;
        }
    }
    else if (idx.key_kind == LL_KEY) {
        for (int i = 0; i < MAXNUM_DATA; ++i) {
            long long *temp = new long long();
            if (fread(temp, sizeof(long long), 1, file) != 1) {
                perror("Failed to read data");
                fclose(file);
                return;
            }
            value[i] = (void*)temp;
        }
    }
    else {
        for (int i = 0; i < MAXNUM_DATA; ++i) {
            char* temp=new char[1024];
            if (fread(temp, sizeof(char), idx.max_size, file) != idx.max_size) {
                perror("Failed to read data");
                fclose(file);
                return;
            }
            value[i] = temp;
        }
    }
    fclose(file);
}

void FileManager::get_BlockGraph(const char* fname, char* freeBlock) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(fname, 1);  // Block graph is in the 1st block
    
    if (page) {
        memcpy(freeBlock, page->data, NUM_ALL_BLOCK);
        bp->unpinPage(fname, 1, false);
    }
    
    // To prevent issues, used blocks exceed actual file size
    size_t i = getFileSize(fname);
    i++;
    for (int j = i; j < NUM_ALL_BLOCK; j++) {
        freeBlock[j] = BLOCK_UNAVA;
    }
}

char FileManager::get_BlockType(const char* fname, off_t offt) {
    char* BlockGRAPH = new char[NUM_ALL_BLOCK];
    get_BlockGraph(fname, BlockGRAPH);
    char type = BlockGRAPH[offt];
    delete[] BlockGRAPH;
    return type;
}

void FileManager::flush_BlockGraph(Index idx, char* freeBlock) {
    BufferPool* bp = BufferPool::getInstance();
    // idx.offt_self here should be LOC_GRAPH (i.e., 1), which is a block index
    Page* page = bp->getPage(idx.fpath, idx.offt_self);
    
    if (page) {
        memcpy(page->data, freeBlock, idx.max_size);

        // Write WAL log 
		// Allocation Graph is crucial
        std::string table_name = getTableNameFromPath(idx.fpath);
        WAL::getInstance()->logInsert(table_name, idx.offt_self, page->data, DB_BLOCK_SIZE);

        bp->unpinPage(idx.fpath, idx.offt_self, true);
    }
}

off_t FileManager::getFreeBlock(const char* fname, char type_block) {
    char* BlockGRAPH = new char[NUM_ALL_BLOCK];
    get_BlockGraph(fname, BlockGRAPH);
    
    for (int i = 0; i < NUM_ALL_BLOCK; i++) {
        if (BlockGRAPH[i] == BLOCK_FREE) {
            Index index(fname, LOC_GRAPH, NUM_ALL_BLOCK, 1);
            BlockGRAPH[i] = type_block;
            flush_BlockGraph(index, BlockGRAPH);
            delete[] BlockGRAPH;
            return i;
        }
    }
    
    int i = this->newBlock(fname);
    BlockGRAPH[i] = type_block;
    Index index(fname, LOC_GRAPH, NUM_ALL_BLOCK, 1);
    flush_BlockGraph(index, BlockGRAPH);
    delete[] BlockGRAPH;
    return i;
}

void FileManager::flush_key(void* key[MAXNUM_KEY], Index idx) {
    FILE* file = fopen(idx.fpath, "rb+");
    if (fseek(file, idx.offt_self, SEEK_SET) != 0) {
        perror("Failed to seek");
        fclose(file);
    }
    if (idx.key_kind == INT_KEY) {
        int temp[MAXNUM_KEY];
        for (int i = 0; i < MAXNUM_KEY; i++) {
            temp[i] = *(int*)key[i];
        }
        fwrite(&temp, sizeof(int), MAXNUM_KEY, file);
    }
    else if (idx.key_kind == LL_KEY) {
        long long temp[MAXNUM_KEY];
        for (int i = 0; i < MAXNUM_KEY; i++) {
            temp[i] = *(long long*)key[i];
        }
        fwrite(&temp, sizeof(long long), MAXNUM_KEY, file);
    }
    else {
        for (int i = 0; i < MAXNUM_KEY; i++) {
            char* temp = (char*)key[i];
            fwrite(temp, sizeof(char), idx.max_size, file);
        }
    }
    fclose(file);
}

bool FileManager::table_create(const char* path, size_t attr_num, attribute attr[ATTR_MAX_NUM]) {
    size_t size = 0;
    int max_key_size = 0;
    KEY_KIND key_type = INT_KEY;
    
    switch (key_type) {
    case INT_KEY:
        size = sizeof(int);
        break;
    case LL_KEY:
        size = sizeof(long long);
        break;
    case STRING_KEY:
        size = max_key_size;
        break;
    default:
        size = max_key_size;
        break;
    }

    FILE* file = fopen(path, "r");
    if (file) {
        fclose(file);
        remove(path);
    }
    
    for(int i = 0; i < 6; i++) {
        newBlock(path);
    }
    
    table t;
    strcpy(t.fpath, path);
    t.key_kind = key_type;
    t.m_Depth = 1;
    t.offt_root = 2;
    t.key_use_block = 1;
    t.value_use_block = 0;
    for(int i = 0; i < attr_num; i++) {
        t.attr[i] = attr[i];
    }
    t.attr_num = attr_num;
    t.max_key_size = size;

    flushTable(t, t.fpath, 0);

    char block_graph[NUM_ALL_BLOCK];
    block_graph[0] = BLOCK_TLABE;
    block_graph[1] = BLOCK_GRAPH;
    for(int j = 2; j < 6; j++) {
        block_graph[j] = BLOCK_FREE;
    }
    for (int j = 6; j < NUM_ALL_BLOCK; j++) {
        block_graph[j] = BLOCK_UNAVA;
    }
    
    Index index(path, LOC_GRAPH, NUM_ALL_BLOCK, 1);
    flush_BlockGraph(index, block_graph);
    
    return true;
}

off_t FileManager::newBlock(const char* filename) {
    FILE* file = fopen(filename, "ab");
    if(!file){
        file = fopen(filename, "wb");
    }
    char zero[DB_BLOCK_SIZE] = { 0 };
    fwrite(zero, 1, sizeof(zero), file);
    off_t now = ftell(file) / DB_BLOCK_SIZE;
    now--;
    fclose(file);
    
    char* graph = new char[NUM_ALL_BLOCK];
    get_BlockGraph(filename, graph);
    graph[now] = BLOCK_FREE;
    Index index(filename, LOC_GRAPH, NUM_ALL_BLOCK, 1);
    flush_BlockGraph(index, graph);
    delete[] graph;
    
    return now;
}

size_t FileManager::getFileSize(const char* fileName) {
    struct stat statbuf;
    size_t ans = stat(fileName, &statbuf);
    
    if (ans == -1) return -1;
    
    size_t filesize = statbuf.st_size;
    return filesize / DB_BLOCK_SIZE;
}

bool FileManager::flushBlock(const char* filename, off_t offt, char type) {
    char* block_graph = new char[NUM_ALL_BLOCK];
    get_BlockGraph(filename, block_graph);
    if(offt < 0 || offt >= NUM_ALL_BLOCK) {
        delete[] block_graph;
        return false;
    }
    block_graph[offt] = type;
    Index index(filename, LOC_GRAPH, NUM_ALL_BLOCK, 1);
    flush_BlockGraph(index, block_graph);
    delete[] block_graph;
    return true;
}

bool FileManager::flush_data(const char* filename, void* data[ATTR_MAX_NUM], 
                             attribute attr[ATTR_MAX_NUM], int attrnum, off_t offt) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(filename, offt);
    
    if (!page) {
        return false;
    }
    
    off_t offset = 0;
    for(int i = 0; i < attrnum; i++) {
        if(attr[i].key_kind == INT_KEY) {
            int data_int = *(int*)data[i];
            memcpy(page->data + offset, &data_int, sizeof(int));
            offset += sizeof(int);
        }
        else if(attr[i].key_kind == LL_KEY) {
            long long data_long = *(long long*)data[i];
            memcpy(page->data + offset, &data_long, sizeof(long long));
            offset += sizeof(long long);
        }
        else if(attr[i].key_kind == STRING_KEY) {
            char* data_string = (char*)data[i];
            memcpy(page->data + offset, data_string, attr[i].max_size);
            offset += attr[i].max_size;
        }
    }

    std::string table_name = getTableNameFromPath(filename);
    WAL::getInstance()->logInsert(table_name, offt, page->data, DB_BLOCK_SIZE);
    
    bp->unpinPage(filename, offt, true);
    return true;
}

void FileManager::get_data(const char* filename, void* data[ATTR_MAX_NUM], 
                           attribute attr[ATTR_MAX_NUM], int attrnum, off_t offt) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(filename, offt);
    
    if (!page) {
        return;
    }
    
    off_t offset = 0;
    for(int i = 0; i < attrnum; i++) {
        if(attr[i].key_kind == INT_KEY) {
            int* data_int = new int();
            memcpy(data_int, page->data + offset, sizeof(int));
            data[i] = data_int;
            offset += sizeof(int);
        }
        else if(attr[i].key_kind == LL_KEY) {
            long long* data_long = new long long();
            memcpy(data_long, page->data + offset, sizeof(long long));
            data[i] = data_long;
            offset += sizeof(long long);
        }
        else if(attr[i].key_kind == STRING_KEY) {
            char* data_string = new char[1024];
            memcpy(data_string, page->data + offset, attr[i].max_size);
            data[i] = data_string;
            offset += attr[i].max_size;
        }
    }
    
    bp->unpinPage(filename, offt, false);
}

bool FileManager::deleteFile(const char* filename) {
    // Flush all dirty pages first
    BufferPool::getInstance()->flushAllPages();
    
    FILE* file = fopen(filename, "w");
    fclose(file);
    return true;
}

database FileManager::getDatabase(const std::string& fname) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(fname.c_str(), 0);
    
    database db;
    if (page) {
        memcpy(&db, page->data, sizeof(database));
        bp->unpinPage(fname.c_str(), 0, false);
    }
    
    return db;
}

bool FileManager::flushDatabase(const std::string& fname, database db) {
    BufferPool* bp = BufferPool::getInstance();
    Page* page = bp->getPage(fname.c_str(), 0);
    
    if (!page) {
        // If file does not exist, create it
        FILE* file = fopen(fname.c_str(), "wb");
        if (file) {
            char zero[DB_BLOCK_SIZE] = {0};
            fwrite(zero, 1, DB_BLOCK_SIZE, file);
            fclose(file);
            page = bp->getPage(fname.c_str(), 0);
        }
    }
    
    if (page) {
        memcpy(page->data, &db, sizeof(database));
        bp->unpinPage(fname.c_str(), 0, true);
        // Usually no WAL here, as metadata files are independent of WAL recovery process
        return true;
    }
    
    return false;
}
