#pragma once
#include "rwdata.h"
#include <string>
#include <vector>
#include <ctime>
#include <cstring> 
#include <cstdint> 

#define WAL_FILE_SUFFIX ".wal"
#define CHECKPOINT_FILE_SUFFIX ".ckpt"

namespace SQL {

// Log record types
enum LogType {
    LOG_BEGIN_TRANS = 1,    // Begin transaction
    LOG_COMMIT = 2,         // Commit transaction
    LOG_ABORT = 3,          // Abort transaction
    LOG_INSERT = 4,         // Insert operation
    LOG_DELETE = 5,         // Delete operation
    LOG_UPDATE = 6,         // Update operation
    LOG_CHECKPOINT = 7      // Checkpoint marker
};

// Log record structure
struct LogRecord {
    uint64_t lsn;           // Log Sequence Number
    uint32_t txn_id;        // Transaction ID (for future use)
    LogType type;           // Operation type
    time_t timestamp;       // Timestamp
    char table_name[100];   // Table affected
    off_t page_offset;      // Page/block offset
    size_t data_size;       // Size of data
    // After this: variable-length data
    
    LogRecord() : lsn(0), txn_id(0), type(LOG_BEGIN_TRANS), timestamp(0), page_offset(0), data_size(0) {
        memset(table_name, 0, 100);
    }
};

class WAL {
private:
    static WAL* instance;
    std::string wal_path;
    uint64_t current_lsn;
    FILE* wal_file;
    bool is_recovering;
    
    WAL();
    
    // Write a log record to disk
    bool writeLogRecord(const LogRecord& record, const void* data = nullptr);
    
    // Read next log record from file
    bool readLogRecord(FILE* file, LogRecord& record, void** data);
    
public:
    static WAL* getInstance();
    
    // Initialize WAL for a database
    bool init(const std::string& db_name);
    
    // Log operations
    uint64_t logInsert(const std::string& table_name, off_t page_offset, const void* data, size_t size);
    uint64_t logDelete(const std::string& table_name, off_t page_offset, const void* old_data, size_t size);
    uint64_t logUpdate(const std::string& table_name, off_t page_offset, 
                       const void* old_data, const void* new_data, size_t size);
    
    // Transaction operations (simplified - auto-commit mode)
    uint64_t logBeginTransaction(uint32_t txn_id = 0);
    uint64_t logCommit(uint32_t txn_id = 0);
    uint64_t logAbort(uint32_t txn_id = 0);
    
    // Checkpoint operations
    bool checkpoint();
    
    // Recovery operations
    bool recover(const std::string& db_name);
    
    // Flush WAL to disk
    void flush();
    
    // Close WAL
    void close();
    
    // Check if system is in recovery mode
    bool isRecovering() const { return is_recovering; }
    
    ~WAL();
};

// Recovery manager
class RecoveryManager {
private:
    static RecoveryManager* instance;
    WAL* wal;
    std::string current_db;
    
    RecoveryManager();
    
public:
    static RecoveryManager* getInstance();
    
    // Initialize recovery for a database
    bool init(const std::string& db_name);
    
    // Perform recovery if needed
    bool performRecovery();
    
    // Create checkpoint
    bool createCheckpoint();
    
    // Check if recovery is needed
    bool needsRecovery(const std::string& db_name);
    
    // Clean up old WAL files
    void cleanupWAL();
    
    ~RecoveryManager();
};

} // namespace SQL