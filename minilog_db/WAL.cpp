#include "WAL.h"
#include "BufferPool.h"
#include "rwdata.h" 
#include <iostream>
#include <cstdio>
#include <cstring>
#include <sys/stat.h>
#include <unistd.h> 

namespace SQL {

// WAL Implementation
WAL* WAL::instance = nullptr;

WAL::WAL() : current_lsn(1), wal_file(nullptr), is_recovering(false) {}

WAL* WAL::getInstance() {
    if (instance == nullptr) {
        instance = new WAL();
    }
    return instance;
}

bool WAL::init(const std::string& db_name) {
    wal_path = db_name + WAL_FILE_SUFFIX;
    
    // Check if WAL file exists
    struct stat st;
    bool wal_exists = (stat(wal_path.c_str(), &st) == 0);
    
    if (!wal_exists) {
        // Create new WAL file
        wal_file = fopen(wal_path.c_str(), "wb+");
        if (!wal_file) {
            std::cerr << "Failed to create WAL file: " << wal_path << std::endl;
            return false;
        }
        current_lsn = 1;
    } else {
        // Open existing WAL file in append mode
        wal_file = fopen(wal_path.c_str(), "ab+");
        if (!wal_file) {
            std::cerr << "Failed to open WAL file: " << wal_path << std::endl;
            return false;
        }
        
        // Find the last LSN
        fseek(wal_file, 0, SEEK_END);
        long file_size = ftell(wal_file);
        
        if (file_size > 0) {
            // Read through the file to find the last LSN
            // I know that in a real production system, this should be read backwards from the end or use an index.
            // Scanning the entire large log file is inefficient, but sufficient for this db.
            rewind(wal_file);
            LogRecord record;
            void* data = nullptr;
            uint64_t max_lsn = 0;
            
            while (readLogRecord(wal_file, record, &data)) {
                if (record.lsn > max_lsn) {
                    max_lsn = record.lsn;
                }
                if (data) {
                    free(data);
                    data = nullptr;
                }
            }
            
            current_lsn = max_lsn + 1;
            
            // Reset file pointer to end for appending
            fseek(wal_file, 0, SEEK_END);
        }
    }
    
    return true;
}

bool WAL::writeLogRecord(const LogRecord& record, const void* data) {
    if (!wal_file) return false;
    
    // Write fixed-size record header
    size_t written = fwrite(&record, sizeof(LogRecord), 1, wal_file);
    if (written != 1) {
        return false;
    }
    
    // Write variable-length data if present
    if (data && record.data_size > 0) {
        written = fwrite(data, 1, record.data_size, wal_file);
        if (written != record.data_size) {
            return false;
        }
    }
    
    return true;
}

bool WAL::readLogRecord(FILE* file, LogRecord& record, void** data) {
    if (!file) return false;
    
    // Read fixed-size record header
    size_t read = fread(&record, sizeof(LogRecord), 1, file);
    if (read != 1) {
        return false;
    }
    
    // Read variable-length data if present
    if (record.data_size > 0) {
        *data = malloc(record.data_size);
        if (!*data) {
            return false;
        }
        
        read = fread(*data, 1, record.data_size, file);
        if (read != record.data_size) {
            free(*data);
            *data = nullptr;
            return false;
        }
    } else {
        *data = nullptr;
    }
    
    return true;
}

uint64_t WAL::logInsert(const std::string& table_name, off_t page_offset, const void* data, size_t size) {
    if (is_recovering) return 0; // Don't log during recovery
    
    LogRecord record;
    record.lsn = current_lsn++;
    record.type = LOG_INSERT;
    record.timestamp = time(nullptr);
    strncpy(record.table_name, table_name.c_str(), 99);
    record.page_offset = page_offset;
    record.data_size = size;
    
    if (writeLogRecord(record, data)) {
        flush(); // Immediate flush for durability (WAL Protocol)
        return record.lsn;
    }
    
    return 0;
}

uint64_t WAL::logDelete(const std::string& table_name, off_t page_offset, const void* old_data, size_t size) {
    if (is_recovering) return 0;
    
    LogRecord record;
    record.lsn = current_lsn++;
    record.type = LOG_DELETE;
    record.timestamp = time(nullptr);
    strncpy(record.table_name, table_name.c_str(), 99);
    record.page_offset = page_offset;
    record.data_size = size;
    
    if (writeLogRecord(record, old_data)) {
        flush();
        return record.lsn;
    }
    
    return 0;
}

uint64_t WAL::logUpdate(const std::string& table_name, off_t page_offset, 
                        const void* old_data, const void* new_data, size_t size) {
    if (is_recovering) return 0;
    
    LogRecord record;
    record.lsn = current_lsn++;
    record.type = LOG_UPDATE;
    record.timestamp = time(nullptr);
    strncpy(record.table_name, table_name.c_str(), 99);
    record.page_offset = page_offset;
    record.data_size = size * 2; // Both old and new data
    
    // Combine old and new data
    char* combined = new char[size * 2];
    memcpy(combined, old_data, size);
    memcpy(combined + size, new_data, size);
    
    bool success = writeLogRecord(record, combined);
    delete[] combined;
    
    if (success) {
        flush();
        return record.lsn;
    }
    
    return 0;
}

uint64_t WAL::logBeginTransaction(uint32_t txn_id) {
    if (is_recovering) return 0;
    
    LogRecord record;
    record.lsn = current_lsn++;
    record.txn_id = txn_id;
    record.type = LOG_BEGIN_TRANS;
    record.timestamp = time(nullptr);
    record.data_size = 0;
    
    if (writeLogRecord(record, nullptr)) {
        flush();
        return record.lsn;
    }
    
    return 0;
}

uint64_t WAL::logCommit(uint32_t txn_id) {
    if (is_recovering) return 0;
    
    LogRecord record;
    record.lsn = current_lsn++;
    record.txn_id = txn_id;
    record.type = LOG_COMMIT;
    record.timestamp = time(nullptr);
    record.data_size = 0;
    
    if (writeLogRecord(record, nullptr)) {
        flush();
        return record.lsn;
    }
    
    return 0;
}

uint64_t WAL::logAbort(uint32_t txn_id) {
    if (is_recovering) return 0;
    
    LogRecord record;
    record.lsn = current_lsn++;
    record.txn_id = txn_id;
    record.type = LOG_ABORT;
    record.timestamp = time(nullptr);
    record.data_size = 0;
    
    if (writeLogRecord(record, nullptr)) {
        flush();
        return record.lsn;
    }
    
    return 0;
}

bool WAL::checkpoint() {
    if (is_recovering) return false;
    
    // 1. Force BufferPool to flush all dirty pages to disk
    BufferPool::getInstance()->flushAllPages();
    
    // 2. Write checkpoint record to WAL
    LogRecord record;
    record.lsn = current_lsn++;
    record.type = LOG_CHECKPOINT;
    record.timestamp = time(nullptr);
    record.data_size = 0;
    
    if (writeLogRecord(record, nullptr)) {
        flush();
        
        // 3. Create/Update separate checkpoint file to store the LSN
        std::string ckpt_file = wal_path + CHECKPOINT_FILE_SUFFIX;
        FILE* ckpt = fopen(ckpt_file.c_str(), "w");
        if (ckpt) {
            fprintf(ckpt, "%lu\n", record.lsn);
            fclose(ckpt);
        }
        
        return true;
    }
    
    return false;
}

bool WAL::recover(const std::string& db_name) {
    std::cout << ">>> Starting recovery for database: " << db_name << std::endl;
    is_recovering = true;
    
    std::string wal_file_path = db_name + WAL_FILE_SUFFIX;
    FILE* recovery_file = fopen(wal_file_path.c_str(), "rb");
    
    if (!recovery_file) {
        std::cout << "No WAL file found, skipping recovery" << std::endl;
        is_recovering = false;
        return true; // No WAL means clean shutdown or fresh DB
    }
    
    // Get Checkpoint LSN
    uint64_t checkpoint_lsn = 0;
    std::string ckpt_file = wal_file_path + CHECKPOINT_FILE_SUFFIX;
    FILE* ckpt = fopen(ckpt_file.c_str(), "r");
    if (ckpt) {
        fscanf(ckpt, "%lu", &checkpoint_lsn);
        fclose(ckpt);
        std::cout << "Found checkpoint at LSN: " << checkpoint_lsn << std::endl;
    }
    
    // Phase 1: Analysis - scan log from beginning or optimized seek
    std::cout << "Phase 1: Analysis (Scanning WAL...)" << std::endl;
    rewind(recovery_file);
    
    LogRecord record;
    void* data = nullptr;
    std::vector<LogRecord> redo_list;
    std::vector<void*> redo_data;
    
    while (readLogRecord(recovery_file, record, &data)) {
        // Only Redo operations that happened after the last checkpoint
        if (record.lsn > checkpoint_lsn) {
            // We only care about modification logs for REDO
            if (record.type == LOG_INSERT || record.type == LOG_UPDATE) {
                redo_list.push_back(record);
                // Take ownership of data pointer
                redo_data.push_back(data);
                data = nullptr; 
            } else if (data) {
                free(data);
                data = nullptr;
            }
        } else if (data) {
            free(data);
            data = nullptr;
        }
    }
    
    fclose(recovery_file);
    
    // Phase 2: Redo - apply changes to pages
    std::cout << "Phase 2: Redo - replaying " << redo_list.size() << " operations" << std::endl;
    
    BufferPool* bp = BufferPool::getInstance();
    int success_count = 0;

    for (size_t i = 0; i < redo_list.size(); i++) {
        LogRecord& rec = redo_list[i];
        void* rec_data = redo_data[i];
        
        // Construct full table filename (e.g., "test.bin")
        // Here we assume the table name matches the filename with a .bin suffix, consistent with tiny_db naming rules
        std::string filename = std::string(rec.table_name) + ".bin";
        
        // Get page via BufferPool
        Page* page = bp->getPage(filename.c_str(), rec.page_offset);
        
        if (page) {
            std::cout << "  [REDO] LSN " << rec.lsn << " Type " << rec.type 
                      << " on " << filename << " (Block " << rec.page_offset << ")" << std::endl;

            // Perform Physical Redo
            // The logic here is based on flush_data in rwdata.cpp, assuming the log records the content of the entire data block
            if (rec.type == LOG_INSERT) {
                // Insert: directly write data from log to page
                // this db's data block structure is simple, data is packed, so we overwrite directly
                memcpy(page->data, rec_data, rec.data_size);
            } 
            else if (rec.type == LOG_UPDATE) {
                // Update: log contains [OldData | NewData]
                // We only need to apply NewData
                size_t single_data_size = rec.data_size / 2;
                char* new_data_ptr = (char*)rec_data + single_data_size;
                
                memcpy(page->data, new_data_ptr, single_data_size);
            }

            // Mark page as dirty, BufferPool will flush it to disk later
            // true in unpinPage(..., true) means is_dirty
            bp->unpinPage(filename.c_str(), rec.page_offset, true);
            success_count++;
        } else {
             std::cerr << "  [ERROR] Failed to load page " << rec.page_offset 
                       << " for table " << filename << std::endl;
        }
        
        if (rec_data) {
            free(rec_data);
        }
    }
    
    // Phase 3: Persistence
    // Force flush all redone dirty pages to disk to ensure durability
    std::cout << "Phase 3: Persistence - Flushing " << success_count << " pages to disk." << std::endl;
    bp->flushAllPages();

    // Phase 4: Cleanup
    // Clear WAL file (equivalent to an implicit Checkpoint), ready for new operations
    wal_file = fopen(wal_file_path.c_str(), "wb");
    if (wal_file) {
        fclose(wal_file);
        wal_file = nullptr;
    }
    
    // Checkpoint file can also be reset or deleted
    // remove(ckpt_file.c_str()); 
    // A better approach is to write a new Checkpoint recording the current LSN; for simplicity, we delete it directly here
    
    is_recovering = false;
    std::cout << "Recovery completed successfully." << std::endl;
    
    return true;
}

void WAL::flush() {
    if (wal_file) {
        fflush(wal_file);
        // In a production environment, fsync(fileno(wal_file)) should be called here to ensure OS flush
    }
}

void WAL::close() {
    if (wal_file) {
        fflush(wal_file);
        fclose(wal_file);
        wal_file = nullptr;
    }
}

WAL::~WAL() {
    close();
}

// RecoveryManager Implementation 
RecoveryManager* RecoveryManager::instance = nullptr;

RecoveryManager::RecoveryManager() : wal(nullptr) {}

RecoveryManager* RecoveryManager::getInstance() {
    if (instance == nullptr) {
        instance = new RecoveryManager();
    }
    return instance;
}

bool RecoveryManager::init(const std::string& db_name) {
    current_db = db_name;
    wal = WAL::getInstance();
    
    // Check if recovery is needed
    if (needsRecovery(db_name)) {
        return performRecovery();
    }
    
    // Initialize WAL for normal operation
    return wal->init(db_name);
}

bool RecoveryManager::performRecovery() {
    if (!wal) return false;
    
    // Perform recovery
    bool success = wal->recover(current_db);
    
    if (success) {
        // Reinitialize WAL for normal operation
        return wal->init(current_db);
    }
    
    return false;
}

bool RecoveryManager::createCheckpoint() {
    if (!wal) return false;
    return wal->checkpoint();
}

bool RecoveryManager::needsRecovery(const std::string& db_name) {
    // Check if WAL file exists and is nonempty
    std::string wal_path = db_name + WAL_FILE_SUFFIX;
    
    struct stat st;
    if (stat(wal_path.c_str(), &st) == 0) {
        return st.st_size > 0;
    }
    
    return false;
}

void RecoveryManager::cleanupWAL() {
    if (wal) {
        wal->close();
    }
    
    // Remove old WAL files if they exist
    std::string wal_path = current_db + WAL_FILE_SUFFIX;
    std::string ckpt_path = wal_path + CHECKPOINT_FILE_SUFFIX;
    
    remove(wal_path.c_str());
    remove(ckpt_path.c_str());
}

RecoveryManager::~RecoveryManager() {
    if (wal) {
        wal->close();
    }
}

} // namespace SQL