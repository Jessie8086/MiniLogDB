#include "DataBase.h"
#include "BPTree.h"
#include "rwdata.h"
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <ostream>
#include <string>
#include <sys/types.h>
#include <vector>
#include <limits> // for numeric_limits
#include "BufferPool.h"
#include "WAL.h"

DataBase::DataBase(){

}

DataBase::~DataBase(){
    
}

void DataBase::run(){
    this->init();
    
    // Initialize recovery manager for this database
    RecoveryManager* recovery_mgr = RecoveryManager::getInstance();
    if (!recovery_mgr->init(this->db.db_name)) {
        cout << "Failed to initialize recovery manager" << endl;
        return;
    }
    
    cout << "Database [" << this->db.db_name << "] ready with recovery support." << endl;
    cout << "Enter SQL commands or 'exit' to quit." << endl;
    cout << "Enter 'checkpoint' to create a recovery checkpoint." << endl;

    int operation_count = 0;
    const int AUTO_CHECKPOINT_INTERVAL = 100; // Auto-checkpoint every 100 operations

    while(true){
        string sql;
        cout << this->db.db_name << "->";
        
        if (!getline(cin, sql)) {
            break;
        }
        
        if(sql.empty()) continue;

        if(sql=="exit"||sql=="quit"||sql=="\\q"){
            break;
        }
        
        // Special command for manual checkpoint
        if(sql=="checkpoint" || sql=="CHECKPOINT") {
            cout << "Creating checkpoint..." << endl;
            if (recovery_mgr->createCheckpoint()) {
                cout << "Checkpoint created successfully." << endl;
            } else {
                cout << "Failed to create checkpoint." << endl;
            }
            continue;
        }
        
        int pos=sql.find_first_of(" ");
        string cmd;
        if(pos == string::npos) cmd = sql;
        else cmd=sql.substr(0,pos);

        // Process SQL commands
        if(cmd=="create"||cmd=="CREATE"){
            this->createTable(sql);
            operation_count++;
        }
        else if(cmd=="insert"||cmd=="INSERT"||cmd=="Insert"){
            this->insert(sql);
            operation_count++;
        }
        else if(cmd=="select"||cmd=="SELECT"||cmd=="Select"){
            this->select(sql);
        }
        else if(cmd=="delete"||cmd=="Delete"||cmd=="DELETE"){
            this->Delete(sql);
            operation_count++;
        }
        else if(cmd=="update"||cmd=="UPDATE"||cmd=="Update"){
            this->Update(sql);
            operation_count++;
        }
        else if(cmd=="drop"||cmd=="DROP"||cmd=="Drop"){
            this->Drop(sql);
            operation_count++;
        }
        else if(cmd=="show"||cmd=="SHOW"||cmd=="Show"){
            this->printTableNames();
        }
        
        // Auto-checkpoint after certain number of operations
        if (operation_count >= AUTO_CHECKPOINT_INTERVAL) {
            cout << "Auto-checkpoint triggered..." << endl;
            if (recovery_mgr->createCheckpoint()) {
                cout << "Auto-checkpoint successful." << endl;
                operation_count = 0;
            }
        }
    }

    // Create final checkpoint before exit
    cout << "Creating final checkpoint..." << endl;
    recovery_mgr->createCheckpoint();
    
    // Save metadata
    string meta_file = string(this->db.db_name) + ".meta";
    FileManager::getInstance()->flushDatabase(meta_file, this->db);
    
    // Flush buffer pool
    BufferPool::getInstance()->flushAllPages();
    
    // Close WAL
    WAL::getInstance()->close();
    
    cout << "Goodbye! All data saved." << endl;
}

void DataBase::init(){
    string db_name_input;
    cout << "Please enter the database name to open or create: ";
    cin >> db_name_input;
    
    // Clear newline from buffer, otherwise it affects getline in run()
    cin.ignore(numeric_limits<streamsize>::max(), '\n'); 

    if(db_name_input.empty()) db_name_input = "default";

    // Construct metadata filename
    string meta_file = db_name_input + ".meta";
    
    // Check if the metadata file for this database exists
    size_t exist = FileManager::getInstance()->getFileSize(meta_file.c_str());
    
    if(exist == -1){
        // Case A: Database does not exist, create new database
        cout << "Database '" << db_name_input << "' does not exist, creating new database..." << endl;
        
        this->db.table_num = 0;
        // Initialize username and password to empty (no longer used)
        memset(this->db.user_name, 0, 100);
        memset(this->db.password, 0, 100);
        strcpy(this->db.db_name, db_name_input.c_str());
        
        // Save immediately to ensure persistence file is created
        FileManager::getInstance()->flushDatabase(meta_file, this->db);
    } 
    else {
        // Case B: Database exists, load old data 
        cout << "Found existing database '" << db_name_input << "', loading data..." << endl;
        this->db = FileManager::getInstance()->getDatabase(meta_file);
        
        // Double check that the loaded name is correct
        strcpy(this->db.db_name, db_name_input.c_str()); 
    }
}

bool DataBase::createTable(const std::string& sql){
    
    string tablename=SQL::extractTableName(sql);
    tablename+=".bin";
    
    // Check if table is already in memory records
    for(int i=0;i<this->db.table_num;i++){
        string existing_name = string(db.tables[i]) + ".bin";
        if(strcmp(existing_name.c_str(), tablename.c_str())==0){
            cout << "Table " << SQL::extractTableName(sql) << " already exists." << endl;
            return false;
        }
    }

    vector<attribute> ture_attr=SQL::parseCreateTableStatement(sql);
    if(ture_attr.size()==0){
        cout<<"Create table failed: Unable to parse attributes" <<endl;
        return false;
    }

    attribute attr[ATTR_MAX_NUM];
    int attr_num=ture_attr.size();
    for(int i=0;i<attr_num;i++){
        attr[i]=ture_attr[i];
    }
    
    // Create physical file
    FileManager::getInstance()->table_create(tablename.c_str(), attr_num, attr);
    
    // Remove .bin suffix and store in table list
    tablename.erase(tablename.size() - 4);
    
    // Update database structure in memory
    strcpy(db.tables[db.table_num], tablename.c_str());
    db.table_num++;
    
    // Flush metadata immediately to prevent table info loss on crash
    string meta_file = string(this->db.db_name) + ".meta";
    FileManager::getInstance()->flushDatabase(meta_file, this->db);
    
    cout << "Table " << tablename << " created successfully." << endl;
    return true;
}

void DataBase::insert(const std::string& sql){
    string fpath=SQL::extractTableName(sql);
    string table_name = fpath;
    fpath+=".bin";
    
    FILE *file=fopen(fpath.c_str(),"r");
    if(!file){
        cout<<"Table does not exist"<<endl;
        return;
    }
    fclose(file);

    vector<vector<string>>values=SQL::parseInsertStatement(sql);
    if(values.empty()) return;

    // Log the start of transaction
    WAL* wal = WAL::getInstance();
    wal->logBeginTransaction();

    BPlusTree* bp=new BPlusTree(fpath);
    
    // Get the data to be inserted and log it
    // This is simplified. In production, we'd log each row
    bool success = bp->Insert_Data(values);
    
    if(success) {
        cout << "Insert successful" << endl;
        // Log commit
        wal->logCommit();
    } else {
        cout << "Insert failed" << endl;
        // Log abort
        wal->logAbort();
    }
    
    bp->flush_file();
    delete bp;
}

void DataBase::select(const std::string& sql){

    string join_table=SQL::extractJoinTableName(sql);
    if(join_table!=""){
        this->selectJoin(sql);
        return;
    }

    string fpath=SQL::extractTableName(sql);
    fpath+=".bin";
    FILE *file=fopen(fpath.c_str(),"r");
    if(!file){
        cout<<"Table does not exist, query failed"<<endl;
        return;
    }
    fclose(file);
    vector<string>attributeNames;
    vector<LOGIC>Logics;
    vector<WhereCondition>whereConditions=SQL::parseSelectStatement(sql,attributeNames,Logics);
    BPlusTree* bp=new BPlusTree(fpath);
    bp->Select_Data(attributeNames, Logics, whereConditions);
    
    delete bp;
}

void DataBase::Delete(const std::string& sql){
    string fpath=SQL::extractTableName(sql);
    string table_name = fpath;
    fpath+=".bin";
    
    FILE *file=fopen(fpath.c_str(),"r");
    if(!file){
        cout<<"Table does not exist"<<endl;
        return;
    }
    fclose(file);
    
    vector<LOGIC>Logics;
    vector<WhereCondition>whereConditions=SQL::parseDeleteStatement(sql,Logics);
    
    // Log the start of transaction
    WAL* wal = WAL::getInstance();
    wal->logBeginTransaction();
    
    BPlusTree* bp=new BPlusTree(fpath);
    bool success = bp->Delete_Data(whereConditions,Logics);
    
    if(success){
        cout<<"Delete successful"<<endl;
        wal->logCommit();
    }
    else{
        cout<<"No matching records found"<<endl;
        wal->logAbort();
    }
    
    bp->flush_file();
    delete bp;
}

void DataBase::Update(const std::string& sql){
    string fpath = SQL::extractTableName(sql);
    string table_name = fpath;
    fpath += ".bin";
    
    FILE *file = fopen(fpath.c_str(), "r");
    if(!file){
        cout << "Table does not exist" << endl;
        return;
    }
    fclose(file);

    vector<WhereCondition> setAttributes;
    vector<WhereCondition> whereConditions = SQL::parseUpdateStatement(sql, setAttributes);

    if (setAttributes.empty()) {
        return;
    }

    // Log the start of transaction
    WAL* wal = WAL::getInstance();
    wal->logBeginTransaction();

    BPlusTree* bp = new BPlusTree(fpath);
    bool success = bp->Update_Data(whereConditions, setAttributes);
    
    if (success) {
        wal->logCommit();
    } else {
        wal->logAbort();
    }
    
    bp->flush_file();
    delete bp;
}

void DataBase::selectJoin(const std::string& sql){
    string fpath1=SQL::extractTableName(sql);
    string fpath2=SQL::extractJoinTableName(sql);
    vector<string>attributeNames;
    vector<LOGIC>Logics;
    vector<WhereCondition>whereConditions=SQL::parseSelectStatement(sql,attributeNames,Logics);

    fpath1+=".bin";
    // No need to add .bin here, because BPlusTree constructor might handle it, or keep it consistent
    // Assuming extractJoinTableName returns pure table name, passing pure table name to fname2 is correct.
    // Based on previous fixes, BPlusTree(fname1, fname2) internally uses fname2 for ref check,
    // and loads using fname2 + ".bin".
    
    BPlusTree* bp1=new BPlusTree(fpath1,fpath2);
    bp1->Select_Data_Join(attributeNames, Logics, whereConditions);
    bp1->flush_file();
    delete bp1;
}

void DataBase::Drop(const std::string& sql){
    string table_name = SQL::extractTableName(sql);
    string fpath = table_name + ".bin";
    
    // Check if file exists
    FILE *file=fopen(fpath.c_str(),"r");
    if(!file){
        cout<<"Table does not exist, delete failed"<<endl;
        return;
    }
    fclose(file);
    
    // Delete physical file
    int res=remove(fpath.c_str());
    if(res!=0){
        cout<<"Failed to delete file"<<endl;
        return;
    }
    
    // Update memory records
    int i=0;
    bool found = false;
    for(;i<db.table_num;i++){
        if(strcmp(this->db.tables[i], table_name.c_str())==0){
            found = true;
            break;
        }
    }
    
    if(found) {
        for(int j=i; j<db.table_num-1; j++){
            strcpy(this->db.tables[j], this->db.tables[j+1]);
        }
        memset(this->db.tables[db.table_num-1], 0, 20);
        db.table_num--;
        
        // Flush metadata immediately
        string meta_file = string(this->db.db_name) + ".meta";
        FileManager::getInstance()->flushDatabase(meta_file, this->db);
        
        cout<<"Delete successful"<<endl;
    } else {
        cout << "Warning: File deleted, but table record not found in metadata." << endl;
    }
}

void DataBase::printTableNames(){
    if(db.table_num==0){
        cout<<"Current database [" << db.db_name << "] is empty (no tables)"<<endl;
        return;
    }
    
    cout << "Tables in database [" << db.db_name << "]:" << endl;
    vector<string>v;
    v.push_back("*");
    
    for(int i=0;i<this->db.table_num;i++){
        string t_name = this->db.tables[i];
        cout << "--- Table: " << t_name << " ---" << endl;
        
        string t_file = t_name + ".bin";
        
        // Simple existence check to prevent crashing when opening non-existent files
        FILE* fp = fopen(t_file.c_str(), "r");
        if(fp) {
            fclose(fp);
            BPlusTree* bp=new BPlusTree(t_file);
            bp->Print_Header(v);
            delete bp;
        } else {
            cout << "(File missing)" << endl;
        }
    }
}

void DataBase::flush(){
    string meta_file = string(this->db.db_name) + ".meta";
    FileManager::getInstance()->flushDatabase(meta_file, this->db);
}