#pragma once
/* (1) A B+ tree of order v consists of a root node, internal nodes, and leaf nodes.
(2) The root node can be a leaf node, or an internal node with two or more subtrees.
(3) Each internal node contains v - 2v keys. If an internal node contains k keys, it has exactly k+1 pointers to subtrees.
(4) Leaf nodes are always on the same level of the tree.
(5) If a leaf node is a primary index, it contains a set of records sorted by key value; if it is a secondary index, it contains a set of short records, each containing a key and a pointer to the actual record.
(6) Key values in internal nodes and data values in leaf nodes are sorted in ascending order.
(7) In an internal node, all keys in the left subtree of a key are smaller than this key, and all keys in the right subtree are greater than or equal to this key.
*/

#include <climits>
#include <cstddef>
#include <iomanip>
#include <ios>
#include <string.h>
#include <sys/types.h>
#define ORDER_V 2    

#define MAXNUM_KEY (ORDER_V * 2)    
#define MAXNUM_POINTER (MAXNUM_KEY + 1)    
#define MAXNUM_DATA (ORDER_V * 2) 
#include<string>
#include<iostream>
#include <sstream>
#include<fstream>
#include<cstring>
#include <vector>
using namespace std;
#include"rwdata.h"


#define TYPE_KEY 0
#define TYPE_VALUE 1

#define INT_KEY 1
#define LL_KEY 2
#define STRING_KEY 3
#define NEW_OFFT 0
typedef int LOGIC;

static int cmp(void* a, void* b,KEY_KIND key_kind) {
    if(key_kind == INT_KEY) {
        return *(int*)a > *(int*)b;
    }
    else if(key_kind == LL_KEY) {
        return *(long long*)a > *(long long*)b;
    }
    int i= strncmp((char *)a, (char *)b, 1024);
    return i>0;
}

static int eql(void* a, void* b,KEY_KIND key_kind) {
    if(key_kind == INT_KEY) {
        return *(int*)a == *(int*)b;
    }
    else if(key_kind == LL_KEY) {
        return *(long long*)a == *(long long*)b;
    }
    // May need modification later
    return strncmp((char *)a, (char *)b, 1024)==0;
}

static void* Invalid(KEY_KIND key_kind) {
    if(key_kind == INT_KEY) {
        return new int(INT_MIN);
    }
    else if(key_kind == LL_KEY) {
        return new long long(LLONG_MIN);
    }

    // char* type definitely needs change later, maybe pass an extra parameter for string length
    char* a=new char[1024];
    for(int i=0;i<6;i++)a[i]='z';
    a[6]='\0';
    return a;
}

static void print_key(void* key, KEY_KIND key_kind) {
    if(key_kind == INT_KEY) {
       cout<<'|'<<*(int*)key<<'\t';
    }
    else if(key_kind == LL_KEY) {
        cout<<'|'<<*(long long*)key<<'\t';
    }
    // char* type definitely needs change later, maybe pass an extra parameter for string length
    else cout<<'|'<<(char*)key<<'\t';
}
static void assign(void* a, void* b, KEY_KIND key_kind) {
    if(key_kind == INT_KEY) {
        a=new int(*(int*)b);
    }
    else if(key_kind == LL_KEY) {
        a=new long long(*(long long*)b);
    }
    // char* might need change
    else a=(char*)b;
    
}
static void* str2value(string value, KEY_KIND key_kind) {
    try{
        int a;
        long long b;
        switch (key_kind) {
            case INT_KEY:
                a=stoi(value);
                return new int(a);
                break;
            case LL_KEY:
                b=stoll(value);
                return new long long(b);
                break;
            case STRING_KEY:
                char *data=new char[1024];
                strcpy((char*)data,value.c_str());
                return data;
                break;

        }
    }
    catch (const std::invalid_argument& ia) {
        //cout << "Invalid argument: " << ia.what() << '\n';
        return nullptr;
    }
    
    return nullptr;
}
static string value2str(void* value, KEY_KIND key_kind) {
    switch (key_kind) {
        case INT_KEY:
            return to_string(*(int*)value);
            break;
        case LL_KEY:
            return to_string(*(long long*)value);
            break;
        case STRING_KEY:
            return (char*)value;
            break;
    }
    return "";
}


// Node data structure, base class for internal nodes and leaf nodes
class CNode
{
public:

    CNode(const char* filename, KEY_KIND key_kind, size_t max_size,off_t offt_self);
    virtual ~CNode();
    // All get functions should new an object, call after reading struct from file. set functions called when flushing to file.
    // Get and Set node type
    NODE_TYPE GetType() { return node_Type; }
    void SetType(NODE_TYPE type) { node_Type = type; }

    // Get and Set valid data count
    int GetCount() { return m_Count; }
    void SetCount(int i) { m_Count = i; }

    // Get and Set an element. For internal nodes it means key, for leaf nodes it means data; need to change to void* later
    virtual void* GetElement(int i) { return 0; }
    virtual void SetElement(int i, void* value) { }

    virtual bool flush_file() { return false; }
    virtual bool get_file() { return false; }
    // Get and Set a pointer. For internal nodes it means pointer, meaningless for leaf nodes
    virtual CNode* GetPointer(int i) { return NULL; }
    virtual void SetPointer(int i, CNode* pointer) { }

    virtual void print_data() { }
    // Get and Set father node, needs offset reading from file
    CNode* GetFather();
    void SetFather(CNode* father) { 
        m_pFather = father;
        if(father != NULL) offt_father = father->getPtSelf();
    }

    off_t getPtFather() {return this->offt_father;}
    void setPtFather(off_t offt) {this->offt_father = offt;}
    // Get the nearest brother node
    CNode* GetBrother(int& flag);
    // Delete node
    void DeleteChildren();
    void FreeBlock() {FileManager::getInstance()->flushBlock(this->fname, this->offt_self, BLOCK_FREE);}
    off_t getPtSelf() {return this->offt_self;}
    void setPtSelf(off_t offt) {this->offt_self = offt;}

protected:

    NODE_TYPE node_Type;    // Node type, value is NODE_TYPE

    KEY_KIND key_kind;    // Key type, value is KEY_KIND

    char fname[100];    // Filename, used to store node data

    size_t max_size;    // Maximum size of node index data

    int m_Count;    // Valid data count, key count for internal nodes, data count for leaf nodes

    CNode* m_pFather;     // Pointer to father node. Standard B+ tree doesn't have this pointer, added for faster split and rotate operations

    off_t offt_father;   // Position of father node data in file

    off_t offt_self;   // Position of node data in file
};

static void updateNode(CNode* node) {
    node->flush_file();
    delete node;
}

// Internal node data structure
class CInternalNode : public CNode
{
public:
    //CInternalNode();
    CInternalNode(const char* fname, KEY_KIND key_kind,size_t max_size,off_t offt);
    virtual ~CInternalNode();

    // Get and Set key value. For user, index starts from 1, actually starts from 0 in node
    void* GetElement(int i);
    void SetElement(int i, void* key);
    CNode* GetPointer(int i);
    
    void SetPointer(int i, CNode* pointer);
    bool Insert(void* value, CNode* pNode);
    bool Delete(void* value);

    // Split node
    void* Split(CInternalNode* pNode, void* key);
    // Merge node
    bool Combine(CNode* pNode);
    // Move one element from another node to this node
    bool MoveOneElement(CNode* pNode);

    // Set pointer data when reading file
    void setPointers(off_t offt_pointer[]) {
        for (int i = 0; i < MAXNUM_POINTER; i++) {
            this->offt_pointers[i] = offt_pointer[i];
        }
    }
    // Return next node's file pointer based on index
    off_t getPointer(int index) {
        return this->offt_pointers[index];
    }

    bool flush_file();

    bool get_file();

    // Function used for testing
    void print_data(){
        cout<<"Offset is: "<<this->offt_self<<" "<<"Internal Node"<<endl;
        for(int i=0;i<MAXNUM_KEY;i++){
            print_key(this->m_Keys[i], this->key_kind);
        }
    }
    
protected:

    void* m_Keys[MAXNUM_KEY];           // Key array
    off_t offt_pointers[MAXNUM_POINTER];     // Pointer array
    
};

// Leaf node data structure
class CLeafNode : public CNode
{
public:

    //CLeafNode();
    CLeafNode(const char* fname,KEY_KIND key_kind,size_t max_size,off_t offt);
        
    
    virtual ~CLeafNode();

    // Get and Set data
    void* GetElement(int i);
    off_t GetElement_offt(int i);
    void SetElement(int i, void* data);
    void SetElement_offt(int i,off_t offt){
        if ((i > 0) && (i <= MAXNUM_KEY))
        {
            this->offt_data[i - 1] = offt;
        }
    }

    // Get and Set pointer. Meaningless for leaf node, just implementing virtual function of base class
    CNode* GetPointer(int i)
    {
        return NULL;
    }

    bool Insert(void* value,off_t offt_data);
    bool Delete(void* ,bool delete_offtdata);

    // Split node
    void* Split(CNode* pNode);
    // Merge node
    bool Combine(CNode* pNode);
    void sePtPrevNode(off_t offset) {
        this->offt_PrevNode = offset;
    }
    void setPtNextNode(off_t offset) {
        this->offt_NextNode = offset;
    }
    // Get previous node offset
    off_t getPrevNodeOffset() const {
        return this->offt_PrevNode;
    }

    // Get next node offset
    off_t getNextNodeOffset() const {
        return this->offt_NextNode;
    }

    bool flush_file();

    bool get_file() ;
    void SetPrevNode(CLeafNode* node);
    CLeafNode* GetPrevNode();
    void SetNextNode(CLeafNode* node);
    CLeafNode* GetNextNode();
    // Function used for testing
    void print_data(){
        cout<<"Offset is: "<<this->getPtSelf()<<" Leaf Node"<<endl;
        for(int i=0;i<MAXNUM_DATA;i++){
            print_key(this->m_Datas[i], this->key_kind);
        }
        cout<<"Data block position is ";
        for(int i=0;i<MAXNUM_DATA;i++){
            cout<<this->offt_data[i]<<" ";
        }
    }
public:
    // Following two variables used for doubly linked list
    CLeafNode* m_pPrevNode;                 // Previous node
    CLeafNode* m_pNextNode;                 // Next node
    off_t offt_PrevNode;                    // Previous node offset in file
    off_t offt_NextNode;                    // Next node offset in file
    off_t offt_data[MAXNUM_DATA];    // Data offset in file
protected:
    void* m_Datas[MAXNUM_DATA];    // Data array, delete after finishing debug
    
};

// B+ Tree data structure
class BPlusTree
{
public:

    BPlusTree();
    // Initialize table (this tree) with filename
    BPlusTree(const std::string& fname);
    BPlusTree(const std::string& fname1, const std::string& fname2);
    virtual ~BPlusTree();
    // Search specific data
    off_t Search(void* data);
    // Insert specific data
    off_t Insert(void* data);
    bool Insert_Data(vector<vector<string>> data);
    void Get_Data(void* data[ATTR_MAX_NUM],off_t offt);
    void Print_Data(void* data[ATTR_MAX_NUM]);
    // Print only one record
    void Print_Data(void* data[ATTR_MAX_NUM],vector<string>attributenames);
    void Print_Data_Join(void* data1[ATTR_MAX_NUM],void* data2[ATTR_MAX_NUM],vector<string>attributenames);
    void Print_Header(vector<string>attributenames);
    void Print_Header_Join(vector<string>attributenames);
    void Select_Data(vector<string>attributenames,vector<LOGIC>Logics,vector<WhereCondition>w);
    void Select_Data_Join(vector<string>attributenames,vector<LOGIC>Logics,vector<WhereCondition>w);
    bool SatisfyCondition(WhereCondition w,void* data[ATTR_MAX_NUM]);
    bool SatisfyConditions(vector<WhereCondition>w,vector<LOGIC>Logics,void* data[ATTR_MAX_NUM]);
    // Delete specific data
    bool Delete(void* data);
    bool Delete_Data(vector<WhereCondition>w,vector<LOGIC>Logics);
    bool Update_Data(vector<WhereCondition>w,vector<WhereCondition>attributenames);
    // Clear tree
    void ClearTree();
    // Print tree
    void PrintTree();
    // Rotate tree
    BPlusTree* RotateTree();
    // Check if tree satisfies B+ tree definition
    bool CheckTree();
    void PrintNode(CNode* pNode);
    // Recursively check if node and its subtrees satisfy B+ tree definition
    bool CheckNode(CNode* pNode);
    CLeafNode* GetLeafHead();
    CLeafNode* GetLeafTail();
    void SetLeafHead(CLeafNode* node);
    void SetLeafTail(CLeafNode* node);
    // Get and Set root node
    CNode* GetRoot();
    void SetRoot(CNode* root);
    // Get and Set depth
    int GetDepth(){return m_Depth;}
    void SetDepth(int depth){m_Depth = depth;}
    void IncDepth(){m_Depth = m_Depth + 1;}
    void DecDepth()
    {
        if (m_Depth > 0)
        {
            m_Depth = m_Depth - 1;
        }
    }

    bool flush_file();
    bool get_file();
    off_t getPtRoot(){return this->offt_root;}

public:
    // Following two variables used for doubly linked list
    CLeafNode* m_pLeafHead;                 // Head node
    CLeafNode* m_pLeafTail;                   // Tail node
    off_t offt_leftHead;
    off_t offt_rightHead;
    char fpath[100];     // File, i.e., table path
    size_t max_key_size;
    CNode* m_Root;    // Root node
    char Block_GRAPH[NUM_ALL_BLOCK];
protected:

    // Search leaf node for insertion
    CLeafNode* SearchLeafNode(void* data);
    // Insert key into internal node
    bool InsertInternalNode(CInternalNode* pNode, void* key, CNode* pRightSon);
    // Delete key from internal node
    bool DeleteInternalNode(CInternalNode* pNode, void* key);
    bool SetCorrentFather(CLeafNode* leaf);
    bool SetCorrentFather(CInternalNode* leaf);
    bool parser_ref_table(const std::string& fname);
    off_t offt_root;    // Offset of root node in file
    int m_Depth;      // Tree depth
    size_t key_use_block;
    size_t value_use_block;
    KEY_KIND key_kind;
    
    off_t offt_self;
    attribute attr[ATTR_MAX_NUM];
    char key_attr[MAXSIZE_ATTR_NAME];
    int attr_num;
    BPlusTree* joinBp;
    string ref_table="None";
    string foreign_key;
    
};

