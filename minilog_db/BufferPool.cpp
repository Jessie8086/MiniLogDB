#include "BufferPool.h"
#include <iostream>
#include <cstdio>

BufferPool* BufferPool::instance = nullptr;

BufferPool::BufferPool() : next_free_page(0) {
    // Initialize page pool
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        pages[i] = Page();
    }
}

BufferPool* BufferPool::getInstance() {
    if (instance == nullptr) {
        instance = new BufferPool();
    }
    return instance;
}

bool BufferPool::loadPage(const char* filename, off_t offset, Page* page) {
    FILE* file = fopen(filename, "rb");
    if (!file) {
        // File does not exist, create new page
        memset(page->data, 0, DB_BLOCK_SIZE);
        strcpy(page->filename, filename);
        page->offset = offset;
        page->is_dirty = false;
        page->pin_count = 0;
        return true;
    }
    
    // Seek to specific block
    if (fseek(file, offset * DB_BLOCK_SIZE, SEEK_SET) != 0) {
        fclose(file);
        return false;
    }
    
    // Read page data
    size_t read = fread(page->data, 1, DB_BLOCK_SIZE, file);
    fclose(file);
    
    // Set page metadata
    strcpy(page->filename, filename);
    page->offset = offset;
    page->is_dirty = false;
    page->pin_count = 0;
    
    return true;
}

bool BufferPool::flushPage(Page* page) {
    if (!page->is_dirty) {
        return true;  // Not dirty, no need to write back
    }
    
    FILE* file = fopen(page->filename, "rb+");
    if (!file) {
        // File does not exist, create file
        file = fopen(page->filename, "wb");
        if (!file) {
            return false;
        }
    }
    
    // Seek to specific block
    if (fseek(file, page->offset * DB_BLOCK_SIZE, SEEK_SET) != 0) {
        fclose(file);
        return false;
    }
    
    // Write page data
    size_t written = fwrite(page->data, 1, DB_BLOCK_SIZE, file);
    fclose(file);
    
    if (written == DB_BLOCK_SIZE) {
        page->is_dirty = false;
        return true;
    }
    
    return false;
}

Page* BufferPool::selectVictim() {
    // Find victim page from the tail of LRU list
    for (auto it = lru_list.rbegin(); it != lru_list.rend(); ++it) {
        Page* page = *it;
        if (page->pin_count == 0) {
            // Found a victim page
            // If dirty, flush to disk first
            if (page->is_dirty) {
                if (!flushPage(page)) {
                    std::cerr << "Failed to flush dirty page" << std::endl;
                    continue;
                }
            }
            
            // Remove old mapping from hash table
            PageId old_id(page->filename, page->offset);
            page_table.erase(old_id);
            
            // Remove from LRU list
            lru_list.erase(std::next(it).base());
            
            return page;
        }
    }
    
    // No victim found (all pages are pinned)
    return nullptr;
}

Page* BufferPool::getPage(const char* filename, off_t offset) {
    PageId pid(filename, offset);
    
    // Check if page is already in buffer pool
    auto it = page_table.find(pid);
    if (it != page_table.end()) {
        // Page in buffer pool, move to head of LRU list
        Page* page = *(it->second);
        lru_list.erase(it->second);
        lru_list.push_front(page);
        page_table[pid] = lru_list.begin();
        
        page->pin_count++;
        return page;
    }
    
    // Page not in buffer pool, need to load from disk
    Page* page = nullptr;
    
    // Check if there are free pages
    if (next_free_page < BUFFER_POOL_SIZE) {
        page = &pages[next_free_page++];
    } else {
        // No free pages, need to replace
        page = selectVictim();
        if (!page) {
            std::cerr << "No available page in buffer pool" << std::endl;
            return nullptr;
        }
    }
    
    // Load page from disk
    if (!loadPage(filename, offset, page)) {
        std::cerr << "Failed to load page from disk" << std::endl;
        return nullptr;
    }
    
    // Add page to head of LRU list
    lru_list.push_front(page);
    page_table[pid] = lru_list.begin();
    
    page->pin_count = 1;
    return page;
}

void BufferPool::unpinPage(const char* filename, off_t offset, bool is_dirty) {
    PageId pid(filename, offset);
    
    auto it = page_table.find(pid);
    if (it != page_table.end()) {
        Page* page = *(it->second);
        
        if (page->pin_count > 0) {
            page->pin_count--;
        }
        
        if (is_dirty) {
            page->is_dirty = true;
        }
    }
}

bool BufferPool::forcePage(const char* filename, off_t offset) {
    PageId pid(filename, offset);
    
    auto it = page_table.find(pid);
    if (it != page_table.end()) {
        Page* page = *(it->second);
        return flushPage(page);
    }
    
    return true;  // Page not in buffer pool, no need to flush
}

void BufferPool::flushAllPages() {
    for (auto& pair : page_table) {
        Page* page = *(pair.second);
        if (page->is_dirty) {
            flushPage(page);
        }
    }
}

BufferPool::~BufferPool() {
    // Flush all dirty pages
    flushAllPages();
}