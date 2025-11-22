#include <iostream>
#include <cstdlib>
#include <unistd.h>
#include <sys/wait.h>
#include <cassert>
#include <string.h>
#include <vector>
#include "BufferPool.h"
#include "DataBase.h"
#include "WAL.h"
#include "BPTree.h"

using namespace std;
using namespace SQL;

const string DB_NAME = "recovery_test_db";
const string TABLE_NAME = "crash_table";

// Helper: Clean up environment
void clean_env() {
    string cmd = "rm -f " + DB_NAME + ".* " + TABLE_NAME + ".*";
    system(cmd.c_str());
}

// Helper: Check if file exists
bool file_exists(const string& name) {
    return access(name.c_str(), F_OK) != -1;
}

// =======================
// Simulate Crash Process
// =======================
void simulate_crash_process() {
    cout << "\n[CrashProcess] PID: " << getpid() << " Executing transaction..." << endl;
    
    DataBase db;
    strcpy(db.db.db_name, DB_NAME.c_str());
    db.db.table_num = 0;

    // 1. Initialize RecoveryManager
    if (!RecoveryManager::getInstance()->init(DB_NAME)) {
        cerr << "[CrashProcess] RecoveryManager initialization failed!" << endl;
        exit(1);
    }

    // 2. Create table
    string create_sql = "CREATE TABLE " + TABLE_NAME + "(id INT PRIMARY KEY, val INT);";
    db.createTable(create_sql);

    // 3. Phase 1: Write base data and perform Checkpoint
    cout << "[CrashProcess] Inserting data id=1, id=2..." << endl;
    db.insert("INSERT INTO " + TABLE_NAME + " (id, val) VALUES(1, 100);");
    db.insert("INSERT INTO " + TABLE_NAME + " (id, val) VALUES(2, 200);");

    cout << "[CrashProcess] >>> Executing Checkpoint (Data id=1,2 will be flushed to disk) <<<" << endl;
    // Manually trigger checkpoint
    if (!RecoveryManager::getInstance()->createCheckpoint()) {
        cerr << "[CrashProcess] Checkpoint failed!" << endl;
        exit(1);
    }

    // 4. Phase 2: Operations after Checkpoint (These data exist only in WAL and memory, not in .bin file)
    cout << "[CrashProcess] Inserting data id=3 (After Checkpoint)..." << endl;
    db.insert("INSERT INTO " + TABLE_NAME + " (id, val) VALUES(3, 300);");

    cout << "[CrashProcess] Updating data id=1 -> val=999 (After Checkpoint)..." << endl;
    // update statement
    db.Update("UPDATE " + TABLE_NAME + " SET val=999 WHERE id=1;");

    // 5. Simulate power failure crash
    // _Exit(0) terminates immediately without calling destructors, dirty pages in BufferPool (id=3 and new value of id=1) will not be flushed to .bin
    cout << "[CrashProcess] Simulating power failure crash (Calling _Exit) " << endl;
    std::_Exit(0); 
}

// ===========================
// Verify Recovery Process
// ===========================
void verify_recovery_process() {
    cout << "\n[VerifyProcess] PID: " << getpid() << " System rebooting..." << endl;

    DataBase db;
    strcpy(db.db.db_name, DB_NAME.c_str());
    
    // 1. Critical: Initialization will trigger recover()
    cout << "[VerifyProcess] Initializing RecoveryManager (Automatically executing Redo)..." << endl;
    bool success = RecoveryManager::getInstance()->init(DB_NAME);
    if (!success) {
        cerr << "[VerifyProcess] Recovery init failed!" << endl;
        exit(1);
    }

    // 2. Verify data
    cout << "[VerifyProcess] Verifying data consistency..." << endl;
    
    string bin_file = TABLE_NAME + ".bin";
    // Reload B+ Tree
    BPlusTree* bp = new BPlusTree(bin_file);
    
    auto check_value = [&](int id, int expected_val, const string& desc) {
        off_t offset = bp->Search(&id);
        if (offset <= 0) {
            cerr << "[Failed] " << desc << ": Cannot find id=" << id << endl;
            exit(1);
        }
        
        // Read data page to verify value
        // Here we simply simulate, trusting Search result exists.
        // To rigorously verify values, we need to read the page data from BufferPool using the offset and parse it.
        // Due to BPlusTree interface limitations, here we mainly verify "record existence" and "metadata correctness".
        // If it's an Update test, we need to read out the value.
        
        // To verify Update value, we use FileManager (it has get_data interface)
        // Note: get_data in rwdata.cpp requires attribute info
        attribute attr[2];
        strcpy(attr[0].name, "id"); attr[0].key_kind = INT_KEY; attr[0].max_size = 4;
        strcpy(attr[1].name, "val"); attr[1].key_kind = INT_KEY; attr[1].max_size = 4;
        
        void* data_ptr[2];
        FileManager::getInstance()->get_data(bin_file.c_str(), data_ptr, attr, 2, offset);
        
        int actual_val = *(int*)data_ptr[1];
        if (actual_val == expected_val) {
            cout << "[Passed] " << desc << ": id=" << id << ", val=" << actual_val << endl;
        } else {
            cerr << "[Failed] " << desc << ": id=" << id << " expected=" << expected_val << " actual=" << actual_val << endl;
            exit(1);
        }
    };

    // Verification Point 1: Data before Checkpoint (Should remain unchanged)
    check_value(2, 200, "Data before Checkpoint (id=2)");

    // Verification Point 2: Insert after Checkpoint (Should be recovered by WAL)
    check_value(3, 300, "Insert after Checkpoint (id=3)");

    // Verification Point 3: Update after Checkpoint (Should be recovered by WAL)
    check_value(1, 999, "Update after Checkpoint (id=1)");

    delete bp;
    cout << "[VerifyProcess] All recovery tests passed!" << endl;
    exit(0);
}

int main() {
    cout << "========================================" << endl;
    cout << "  Database Crash Recovery Test Suite " << endl;
    cout << "========================================" << endl;

    clean_env();

    // 1. Start crash process
    pid_t pid = fork();
    if (pid == 0) {
        simulate_crash_process();
        return 0;
    } else if (pid < 0) {
        cerr << "Fork failed" << endl;
        return 1;
    }

    // Wait for crash
    int status;
    waitpid(pid, &status, 0);
    
    cout << "-----------------------------------------------" << endl;
    cout << "[Main] Database crash detected. Waiting for file system sync (1s)..." << endl;
    cout << "-----------------------------------------------" << endl;
    sleep(1);

    // 2. Start recovery process
    pid = fork();
    if (pid == 0) {
        verify_recovery_process();
        return 0;
    } else if (pid < 0) {
        cerr << "Fork failed" << endl;
        return 1;
    }

    // Wait for recovery verification
    waitpid(pid, &status, 0);

    clean_env();

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        return 0;
    } else {
        return 1;
    }
}