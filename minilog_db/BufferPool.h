#pragma once
#include "rwdata.h"
#include <unordered_map>
#include <list>
#include <mutex>
#include <cstring>

// Buffer pool size (number of pages)
#define BUFFER_POOL_SIZE 100

// Page status
struct Page {
    char data[DB_BLOCK_SIZE];  // Page data
    char filename[100];         // Filename
    off_t offset;              // Offset in file (block number)
    bool is_dirty;             // Dirty page flag
    int pin_count;             // Pin count, cannot be replaced if > 0
    
    Page() : offset(-1), is_dirty(false), pin_count(0) {
        memset(data, 0, DB_BLOCK_SIZE);
        memset(filename, 0, 100);
    }
};

// Page identifier (used as key for hash table)
struct PageId {
    char filename[100];
    off_t offset;
    
    PageId(const char* fname, off_t off) : offset(off) {
        strcpy(filename, fname);
    }
    
    bool operator==(const PageId& other) const {
        return strcmp(filename, other.filename) == 0 && offset == other.offset;
    }
};

// Custom hash function
struct PageIdHash {
    size_t operator()(const PageId& pid) const {
        size_t h1 = std::hash<std::string>{}(pid.filename);
        size_t h2 = std::hash<off_t>{}(pid.offset);
        return h1 ^ (h2 << 1);
    }
};

class BufferPool {
private:
    static BufferPool* instance;
    
    // LRU list (stores page pointers)
    std::list<Page*> lru_list;
    
    // Hash map: PageId -> iterator of the page in LRU list
    std::unordered_map<PageId, std::list<Page*>::iterator, PageIdHash> page_table;
    
    // Page pool
    Page pages[BUFFER_POOL_SIZE];
    int next_free_page;  // Index of the next free page
    
    BufferPool();
    
    // Write page back to disk
    bool flushPage(Page* page);
    
    // Load page from disk
    bool loadPage(const char* filename, off_t offset, Page* page);
    
    // Select a victim page (using LRU strategy)
    Page* selectVictim();
    
public:
    static BufferPool* getInstance();
    
    // Get page (main interface)
    // If the page is in the buffer pool, return it directly; otherwise load it from disk
    Page* getPage(const char* filename, off_t offset);
    
    // Unpin page (decrease pin_count)
    void unpinPage(const char* filename, off_t offset, bool is_dirty);
    
    // Force write page back to disk
    bool forcePage(const char* filename, off_t offset);
    
    // Flush all dirty pages to disk
    void flushAllPages();
    
    ~BufferPool();
};