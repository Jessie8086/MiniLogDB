#include "BPTree.h"
#include "rwdata.h"
#include "stdio.h"
#include "stdlib.h"
#include <climits>
#include <cstring>
#include <iostream>
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <vector>
#ifndef _WIN32
    #include <strings.h>  // for strcasecmp on Linux/macOS
#endif

CNode::CNode(const char* filename, KEY_KIND key_kind, size_t max_size,off_t offt)
{
    memcpy(this->fname, filename, strlen((char*)filename));
    this->fname[strlen((char*)filename)] = '\0';
    this->key_kind = key_kind;
    this->max_size = max_size;
    this->offt_self = offt;
    node_Type = NODE_TYPE_LEAF;
    m_Count = 0;
    m_pFather = NULL;
}
CNode::~CNode()
{
    DeleteChildren();
}

// Get the nearest brother node
CNode* CNode::GetBrother(int& flag)
{
    CNode* pFather = GetFather();   // Get father node pointer
    if (NULL == pFather)
    {
        return NULL;
    }

    CNode* pBrother = NULL;
    for (int i = 1; i <= pFather->GetCount() + 1; i++)   // GetCount() returns number of data or keys, which is 1 less than number of pointers.
    {
        // Find position of current node
        if (pFather->GetPointer(i)->getPtSelf() == this->offt_self)
        {
            if (i == (pFather->GetCount() + 1))   // It is the rightmost child of the father node.
            {
                pBrother = pFather->GetPointer(i - 1);    // It is the last pointer, can only look for the previous pointer
                flag = FLAG_LEFT;
            }
            else
            {
                pBrother = pFather->GetPointer(i + 1);    // Prioritize looking for the next pointer
                flag = FLAG_RIGHT;
            }
        }
    }

    return pBrother;
}

// Recursively delete child nodes
void CNode::DeleteChildren()   // Question: Should pointer index here start from 0
{
    // The +1 here is a bug fix I added
    for (int i = 1; i <= GetCount()+1; i++)   // GetCount() returns number of keys/data in the node
    {
        CNode* pNode = GetPointer(i);
        if (NULL != pNode)    // Leaf nodes do not have pointers
        {
            pNode->DeleteChildren();
        }
        //this->FreeBlock(); // Delete node
        delete pNode;
    }
}
CNode* CNode::GetFather(){
    if(this->offt_father<=LOC_GRAPH||this->offt_father>NUM_ALL_BLOCK) return NULL;
    char type=FileManager::getInstance()->get_BlockType(this->fname, this->offt_father);
    if(type==BLOCK_INTER)return new CInternalNode(this->fname, this->key_kind, this->max_size, this->offt_father);

    else if(type==BLOCK_LEAF)return new CLeafNode(this->fname, this->key_kind, this->max_size, this->offt_father);
    else {
        this->offt_father=INVALID;
        return NULL;
    }
    
}


// Initialize keys and pointers of internal node to 0 and NULL respectively
CInternalNode::CInternalNode(const char* filename, KEY_KIND key_kind, size_t max_size,off_t offt):CNode(filename,  key_kind,  max_size,  offt)
{

    node_Type = NODE_TYPE_INTERNAL;
    // Actually offt here is a bit unsafe, might change later
    for(int i=0; i < MAXNUM_KEY; i++) {
        this->m_Keys[i] = Invalid(this->key_kind);
    }
    for(int i = 0; i < MAXNUM_POINTER; i++)
    {
        this->offt_pointers[i] = INT_MIN;
    }
    if(this->offt_self != NEW_OFFT){
        this->get_file();
    }
    else{
        this->offt_self=FileManager::getInstance()->getFreeBlock(filename, BLOCK_INTER);
        
    
    }
    FileManager::getInstance()->flushBlock(fname, this->offt_self, BLOCK_INTER);

}

CInternalNode::~CInternalNode()
{
    for (int i = 0; i < MAXNUM_POINTER; i++)
    {
        this->offt_pointers[i] = 0;
    }
}

void* CInternalNode::GetElement(int i)
{
        if ((i > 0) && (i <= MAXNUM_KEY))
        {
            return m_Keys[i - 1];
        }
        else
        {
            return INVALID;
        }
}
void CInternalNode::SetElement(int i, void* key)
{
        if ((i > 0) && (i <= MAXNUM_KEY))
        {
            //m_Keys[i - 1] = key;
            if(this->key_kind==INT_KEY){
                this->m_Keys[i - 1] = (void*)(new int(*(int*)key));
            }
            else if(this->key_kind==LL_KEY){
                this->m_Keys[i - 1] = (void*)(new long long(*(long long*)key));
            }
            else if(this->key_kind==STRING_KEY){
                this->m_Keys[i - 1] = (void*)((char*)key);
            }
        }
}
void CInternalNode::SetPointer(int i, CNode* pointer)
{
        if ((i > 0) && (i <= MAXNUM_POINTER))
        {
            if(pointer != NULL)offt_pointers[i - 1] = pointer->getPtSelf();
            else offt_pointers[i - 1] = INVALID;
        }
}
bool CInternalNode::flush_file() {
        inter_node node;
        /* This part will be changed according to data requirements later */
        //memcpy(node.m_Keys, this->m_Keys, sizeof(this->m_Keys));
        memcpy(node.offt_pointers, this->offt_pointers, sizeof(this->offt_pointers));
        node.offt_self = this->offt_self;
        node.offt_father = this->offt_father;
        node.count = this->m_Count;
        node.node_type = this->node_Type;
        
        Index index(this->fname, this->offt_self,  this->max_size,this->key_kind);
        FileManager::getInstance()->flushInterNode(node,index,this->m_Keys);

        return true;
}
bool CInternalNode::get_file() {
        Index index(this->fname, this->offt_self,  this->max_size,this->key_kind);
        // getCInternalNode below to be modified
        inter_node node=FileManager::getInstance()->getCInternalNode(index,this->m_Keys ,this->offt_self);

        memcpy(this->offt_pointers, node.offt_pointers, sizeof(node.offt_pointers));
        this->m_Count = node.count;
        this->node_Type = node.node_type;
        this->offt_father=node.offt_father;

        return true;
}
CNode* CInternalNode::GetPointer(int i)
{
    if ((i > 0) && (i <= MAXNUM_POINTER))
    {
            // Change to pointer reading later
        char type=FileManager::getInstance()->get_BlockType(this->fname, this->offt_pointers[i - 1]);
        if(type==BLOCK_INTER)return new CInternalNode(this->fname, this->key_kind, this->max_size, this->offt_pointers[i - 1]);
        else if(type==BLOCK_LEAF)return new CLeafNode(this->fname, this->key_kind, this->max_size, this->offt_pointers[i - 1]);
        else return NULL;
    }
        
        
    return NULL;
        
}
// Insert key into internal node.
/* Question: Does internal node need to insert value? Usually insertion happens after finding position in leaf node.
Internal node usually inserts two child nodes after splitting when leaf node needs splitting */
bool CInternalNode::Insert(void* value, CNode* pNode)
{
    int i;
    // If internal node is full, return failure directly
    if (GetCount() >= MAXNUM_KEY)
    {
        return false;
    }

    int j = 0;

    // Find position to insert key, comparison rule here needs change, here it's equivalent to >=
    for (i = 0;  (i < m_Count)&&(cmp(value , m_Keys[i],this->key_kind)||eql(value , m_Keys[i],this->key_kind)) ; i++)
    {
    }

    // Current position and subsequent keys shift backward, clearing current position
    for (j = m_Count; j > i; j--)
    {
        m_Keys[j] = (void*)(new int(*(int*)m_Keys[j - 1]));
    }

    // Current position and subsequent pointers shift backward
    for (j = m_Count + 1; j > i + 1; j--)
    {
        this->offt_pointers[j] = this->offt_pointers[j - 1];
    }

    // Store key and pointer in current position
    m_Keys[i] = value;
    this->offt_pointers[i + 1] = pNode->getPtSelf();    // Note it is the (i+1)-th pointer not the i-th
    pNode->SetFather(this);      // Very important. This function means inserting keyword 'value' and the subtree it points to
    pNode->flush_file();
    m_Count++;


    // Return success
    return true;
}

// Delete key in internal node, and the pointer after the key
bool CInternalNode::Delete(void* key)
{
    int i, j, k;
    for (i = 0; (i < m_Count)&&(cmp(key ,m_Keys[i],this->key_kind)||eql(key , m_Keys[i],this->key_kind))  ; i++)
    {
    }

    for (j = i - 1; j < m_Count - 1; j++)
    {
        m_Keys[j] = m_Keys[j + 1];
    }
    m_Keys[j] = Invalid(this->key_kind);

    for (k = i; k < m_Count; k++)
    {
        this->offt_pointers[k] = this->offt_pointers[k + 1];
    }
    this->offt_pointers[k] = 0;

    m_Count--;
    this->flush_file();
    return true;
}

/* Split internal node
Splitting internal node is completely different from splitting leaf node, because internal node has 2V keys and 2V+1 pointers. If simply split into 2, pointers cannot be distributed.
So according to http://www.seanster.com/BplusTree/BplusTree.html, algorithm for splitting internal node is:
Based on the key to be inserted:
(1) If key < V-th key, extract V-th key, its left and right keys are distributed to two nodes respectively
(2) If key > (V+1)-th key, extract (V+1)-th key, its left and right keys are distributed to two nodes respectively
(3) If key is between V-th and (V+1)-th key, extract key, original keys are split half into two nodes
The extracted RetKey is used for subsequent insertion into ancestor node
*/
void* CInternalNode::Split(CInternalNode* pNode, void* key)  // key is the newly inserted value, pNode is the split node
{
    int i = 0, j = 0;

    // If key to insert is between V-th and (V+1)-th key, need to flip, so handle this case first
    if (cmp(key , this->GetElement(ORDER_V),this->key_kind) && cmp( this->GetElement(ORDER_V + 1),key,this->key_kind))
    {
        // Move keys V+1 -- 2V to specified node

        for (i = ORDER_V + 1; i <= MAXNUM_KEY; i++)
        {
            j++;
            pNode->SetElement(j, this->GetElement(i));
            this->SetElement(i, Invalid(this->key_kind));
        }

        // Move pointers V+2 -- 2V+1 to specified node
        j = 0;
        for (i = ORDER_V + 2; i <= MAXNUM_POINTER; i++)
        {
            j++;
            this->GetPointer(i)->SetFather(pNode);    // Reset father of child node
            pNode->SetPointer(j, this->GetPointer(i));
            this->SetPointer(i, INVALID);
        }

        // Set Count
        this->SetCount(ORDER_V);
        pNode->SetCount(ORDER_V);

        // Return original key
        return key;
    }

    // Handle cases where key < V-th key or key > (V+1)-th key

    // Determine whether to extract V-th or (V+1)-th key
    int position = 0;
    // if (key < this->GetElement(ORDER_V))
    // {
    //      position = ORDER_V;
    // }
    if(cmp(this->GetElement(ORDER_V + 1),key,this->key_kind)){
        position = ORDER_V;
    }
    else
    {
        position = ORDER_V + 1;
    }

    // Extract position-th key, return as new key
    void* RetKey = this->GetElement(position);

    // Move keys position+1 -- 2V to specified node
    j = 0;
    for (i = position + 1; i <= MAXNUM_KEY; i++)
    {
        j++;
        pNode->SetElement(j, this->GetElement(i));
        this->SetElement(i, Invalid(this->key_kind));
    }

    // Move pointers position+1 -- 2V+1 to specified node (note pointers are one more than keys)
    j = 0;
    for (i = position + 1; i <= MAXNUM_POINTER; i++)
    {
        j++;
        this->GetPointer(i)->SetFather(pNode);    // Reset father of child node
        pNode->SetPointer(j, this->GetPointer(i));
        this->SetPointer(i, INVALID);
    }

    // Clear extracted position
    this->SetElement(position, Invalid(this->key_kind));

    // Set Count
    this->SetCount(position - 1);
    pNode->SetCount(MAXNUM_KEY - position);


    return RetKey;
}

// Combine node, cut all data from specified internal node to this internal node
bool CInternalNode::Combine(CNode* pNode)
{
    // Parameter check
    if (this->GetCount() + pNode->GetCount() + 1 > MAXNUM_DATA)    // Reserve a position for new key
    {
        return false;
    }

    // Take the first element of the first child of the node to be merged as the new key value
    void* NewKey=NULL;
    CNode* node=pNode->GetPointer(1);
    while(node->GetType()!=NODE_TYPE_LEAF)node=node->GetPointer(1);
    NewKey = node->GetElement(1);
    //assign(NewKey,pNode->GetPointer(1)->GetElement(1) , this->key_kind);
    m_Keys[m_Count] = NewKey;
    m_Count++;
    this->offt_pointers[m_Count] = pNode->GetPointer(1)->getPtSelf();   // Question: Seems it should be m_Pointers[m_Count+1] = pNode->GetPointer(1);

    for (int i = 1; i <= pNode->GetCount(); i++)
    {
        m_Keys[m_Count] = pNode->GetElement(i);
        m_Count++;
        this->offt_pointers[m_Count] = pNode->GetPointer(i + 1)->getPtSelf();
    }
    pNode->FreeBlock();
    return true;
}

// Move one element from another node to this node
bool CInternalNode::MoveOneElement(CNode* pNode)
{
    // Parameter check
    if (this->GetCount() >= MAXNUM_DATA)
    {
        return false;
    }

    int i, j;


    // Brother node is on the left of this node
    
    if (cmp(this->GetElement(1), pNode->GetElement(1),this->key_kind))
    {
        // Make space first
        for (i = m_Count; i > 0; i--)
        {
            m_Keys[i] = m_Keys[i - 1];
        }
        for (j = m_Count + 1; j > 0; j--)
        {
            this->offt_pointers[j] = this->offt_pointers[j - 1];
        }

        // Assignment
        // The first key value is not the last key value of the brother node, but the value of the first element of the first child node of this node
        m_Keys[0] = GetPointer(1)->GetElement(1);
        // The first child node becomes the last child node of the brother node
        this->offt_pointers[0] = pNode->GetPointer(pNode->GetCount() + 1)->getPtSelf();

        // Modify brother node
        pNode->SetElement(pNode->GetCount(), Invalid(this->key_kind));
        pNode->SetPointer(pNode->GetCount() + 1, INVALID);
    }
    else    // Brother node is on the right of this node
    {
        // Assignment
        // The last key value is not the first key value of the brother node, but the value of the first element of the first child node of the brother node
        m_Keys[m_Count] = pNode->GetPointer(1)->GetElement(1);
        // The last child node becomes the first child node of the brother node
        this->offt_pointers[m_Count + 1] = pNode->GetPointer(1)->getPtSelf();

        // Modify brother node
        // for (i = 1; i < pNode->GetCount() - 1; i++)
        // {
        //      pNode->SetElement(i, pNode->GetElement(i + 1));
        // }
        for (i = 1; i < pNode->GetCount(); i++)
        {
            pNode->SetElement(i, pNode->GetElement(i + 1));
        }
        pNode->SetElement(pNode->GetCount(), Invalid(this->key_kind));
        for (j = 1; j <= pNode->GetCount(); j++)
        {
            pNode->SetPointer(j, pNode->GetPointer(j + 1));
        }
    }

    // Set count
    this->SetCount(this->GetCount() + 1);
    pNode->SetCount(pNode->GetCount() - 1);
    this->flush_file();
    pNode->flush_file();
    return true;
}


CLeafNode::CLeafNode(const char* fname,KEY_KIND key_kind,size_t max_size,off_t offt):CNode(fname,key_kind,max_size,offt)
{
    this->node_Type = NODE_TYPE_LEAF;
    m_pPrevNode = NULL;
    m_pNextNode = NULL;
    this->offt_NextNode=0;
    this->offt_PrevNode=0;
    for (int i = 0; i < MAXNUM_DATA; i++)m_Datas[i] = Invalid(this->key_kind);
    for(int i=0;i<MAXNUM_DATA;i++)this->offt_data[i]=INVALID;
    if(this->offt_self != NEW_OFFT){
        this->get_file();
    }
    else{
        this->offt_self=FileManager::getInstance()->getFreeBlock(fname, BLOCK_LEAF);
        // Must remember to update later
        }
    FileManager::getInstance()->flushBlock(fname, this->offt_self, BLOCK_LEAF);
    
}
CLeafNode::~CLeafNode()
{

}
void* CLeafNode::GetElement(int i)
{
        if ((i > 0) && (i <= MAXNUM_DATA))
        {
            return m_Datas[i - 1];
        }
        else
        {
            return INVALID;
        }
}
off_t CLeafNode::GetElement_offt(int i){
        if ((i > 0) && (i <= MAXNUM_DATA))
        {
            return this->offt_data[i - 1];
        }
        else
        {
            return INVALID;
        }
}
void CLeafNode::SetElement(int i, void* data)
{
        if ((i > 0) && (i <= MAXNUM_KEY))
        {
            //m_Keys[i - 1] = key;
            if(this->key_kind==INT_KEY){
                this->m_Datas[i - 1] = (void*)(new int(*(int*)data));
            }
            else if(this->key_kind==LL_KEY){
                this->m_Datas[i - 1] = (void*)(new long long(*(long long*)data));
            }
            else if(this->key_kind==STRING_KEY){
                this->m_Datas[i - 1] = (void*)((char*)data);
            }
        }
}
bool CLeafNode::flush_file() {
        
        leaf_node node(this->offt_self,this->GetCount(),NODE_TYPE_LEAF,this->offt_father,this->offt_PrevNode,this->offt_NextNode,this->offt_data);
        Index index(this->fname,this->offt_self,this->max_size,this->key_kind);
        FileManager::getInstance()->flushLeafNode(node, index,this->m_Datas);

        return true;
}
bool CLeafNode::get_file() {
        
        Index index(this->fname,this->offt_self,this->max_size,this->key_kind);

        leaf_node node = FileManager::getInstance()->getLeafNode(index,this->m_Datas,this->offt_self);

        //memcmp(this->m_Datas, node.m_Datas, sizeof(node.m_Datas));
        this->offt_PrevNode = node.offt_PrevNode;
        this->offt_NextNode = node.offt_NextNode;
        this->offt_father = node.offt_father;
        this->m_Count = node.count;
        this->node_Type = node.node_type;
        this->offt_self=node.offt_self;
        for(int i=0;i<MAXNUM_DATA;i++){
            this->offt_data[i]=node.offt_data[i];
        }
        return true;
}
// Insert data into leaf node
bool CLeafNode::Insert(void* value,off_t offt_data)
{
    int i, j;
    // If leaf node is full, return failure directly
    if (GetCount() >= MAXNUM_DATA)
    {
        return false;
    }

    // Find position to insert data
    for (i = 0; (i < m_Count)&&cmp(value , m_Datas[i],this->key_kind) ; i++)
    {
    }

    // Current position and subsequent data shift backward, clearing current position
    for (j = m_Count; j > i; j--)
    {
        m_Datas[j] = m_Datas[j - 1];
        this->offt_data[j]=this->offt_data[j-1];
    }

    // Store data in current position, the only place for insertion
    m_Datas[i] = value;
    this->offt_data[i]=offt_data;

    m_Count++;
    

    // Return success
    return true;
}

bool CLeafNode::Delete(void* value,bool deleteo_offtData)
{
    int i, j;
    bool found = false;
    for (i = 0; i < m_Count; i++)
    {
        if (eql(value, this->m_Datas[i], this->key_kind))
        {
            found = true;
            break;
        }
    }
    // If not found, return failure
    if (false == found)
    {
        return false;
    }
    if(deleteo_offtData)FileManager::getInstance()->flushBlock(this->fname, this->offt_data[i], BLOCK_FREE);
    // Subsequent data shift forward
    for (j = i; j < m_Count - 1; j++)
    {
        m_Datas[j] = m_Datas[j + 1];
    }
    for(j=i;j<m_Count-1;j++){
        this->offt_data[j]=this->offt_data[j+1];
    }
    // Set the last data as invalid
    m_Datas[j] = Invalid(this->key_kind);
    this->offt_data[j]=INVALID;
    // Release this data block
    
    this->flush_file();
    m_Count--;
    
    
    // Return success
    return true;

}

// Split leaf node, cut the second half of data of this leaf node to specified leaf node
void* CLeafNode::Split(CNode* pNode)
{
    // Move the second half of data of this leaf node to specified node
    int j = 0;
    for (int i = ORDER_V + 1; i <= MAXNUM_DATA; i++)
    {
        j++;
        pNode->SetElement(j, this->GetElement(i));
        ((CLeafNode*)pNode)->SetElement_offt(j, this->GetElement_offt(i));
        this->SetElement(i, Invalid(this->key_kind));
        this->SetElement_offt(i, INVALID);
    }
    // Set Count
    this->SetCount(this->GetCount() - j);
    pNode->SetCount(pNode->GetCount() + j);

    // Return the first element of the new node as key
    return pNode->GetElement(1);
}

// Combine node, cut all data from specified leaf node to this leaf node
bool CLeafNode::Combine(CNode* pNode)
{
    // Parameter check
    if (this->GetCount() + pNode->GetCount() > MAXNUM_DATA)
    {
        return false;
    }

    for (int i = 1; i <= pNode->GetCount(); i++)
    {
        this->Insert(pNode->GetElement(i),((CLeafNode*)pNode)->GetElement_offt(i));
    }
    pNode->FreeBlock();
    return true;
}

void CLeafNode::SetPrevNode(CLeafNode* node){
    this->m_pPrevNode = node;
    if(node!=NULL){this->offt_PrevNode = node->offt_self;}
}
CLeafNode* CLeafNode::GetPrevNode(){
    if(this->offt_PrevNode<=LOC_GRAPH)return NULL;
    char type = FileManager::getInstance()->get_BlockType(this->fname, this->offt_PrevNode);
    if(type!=BLOCK_LEAF)return NULL;
    return new CLeafNode(this->fname, this->key_kind, this->max_size, this->offt_PrevNode);
}
void CLeafNode::SetNextNode(CLeafNode* node){
    this->m_pNextNode = node;
    if(node!=NULL){this->offt_NextNode = node->offt_self;}
    
}
CLeafNode* CLeafNode::GetNextNode(){
    if(this->offt_NextNode<=LOC_GRAPH)return NULL;
    char type = FileManager::getInstance()->get_BlockType(this->fname, this->offt_NextNode);
    if(type!=BLOCK_LEAF)return NULL;
    return new CLeafNode(this->fname, this->key_kind, this->max_size, this->offt_NextNode);
}
BPlusTree::BPlusTree()
{
    m_Depth = 0;
    m_Root = NULL;
    m_pLeafHead = NULL;
    m_pLeafTail = NULL;
    this->offt_self = 0;
    
}
BPlusTree::BPlusTree(const std::string& fname)
{
    strcpy(this->fpath, fname.c_str());
    this->offt_self = 0;
    get_file();
    this->m_Root=NULL;
    this->m_pLeafHead=NULL;
    this->m_pLeafTail=NULL;
    FileManager::getInstance()->get_BlockGraph(this->fpath, this->Block_GRAPH);
    // cout<<"Initialize bitmap: "<<endl;
    // for(int i=0;i<30;i++){
    //      cout<<this->Block_GRAPH[i]<<" ";
    // }
    // cout<<endl;
    if(this->Block_GRAPH[offt_root]==BLOCK_LEAF){
        this->m_Root=new CLeafNode(this->fpath, this->key_kind, this->max_key_size, this->offt_root);

    }
    else if(this->Block_GRAPH[offt_root]==BLOCK_INTER){
        this->m_Root=new CInternalNode(this->fpath, this->key_kind, this->max_key_size, this->offt_root);
    }
    
}
BPlusTree::BPlusTree(const std::string& fname1, const std::string& fname2){
    strcpy(this->fpath, fname1.c_str());
    this->offt_self = 0;
    get_file();
    this->m_Root=NULL;
    this->m_pLeafHead=NULL;
    this->m_pLeafTail=NULL;
    FileManager::getInstance()->get_BlockGraph(this->fpath, this->Block_GRAPH);
    if(this->Block_GRAPH[offt_root]==BLOCK_LEAF){
        this->m_Root=new CLeafNode(this->fpath, this->key_kind, this->max_key_size, this->offt_root);

    }
    else if(this->Block_GRAPH[offt_root]==BLOCK_INTER){
        this->m_Root=new CInternalNode(this->fpath, this->key_kind, this->max_key_size, this->offt_root);
    }
    bool has_ref = parser_ref_table(fname2);
    this->joinBp = new BPlusTree(fname2 + ".bin");
    if (!has_ref) {
        this->ref_table = fname2;
        this->foreign_key = ""; // Mark as not found, need dynamic search
    }
    
}

BPlusTree::~BPlusTree()
{
    ClearTree();
}
bool BPlusTree::parser_ref_table(const std::string& ref_table_name){
    string a="";
    for(int i=0;i<attr_num;i++){
        string t=this->attr[i].constraint;
        if(t.find(ref_table_name)!=string::npos) a=a+t;
    }
    if(a==""){
        return false;
    }
    
    istringstream iss(a);
    string token;
    getline(iss, token, ' '); // "ref"
    getline(iss, this->ref_table, '('); // "tablename"
    getline(iss, this->foreign_key, ')'); // "key_name"
    
    // Remove possible trailing space
    if(!this->ref_table.empty() && this->ref_table.back() == ' ') this->ref_table.pop_back();
    
    return (ref_table_name == this->ref_table);
}
CNode* BPlusTree::GetRoot(){
    char type=FileManager::getInstance()->get_BlockType(this->fpath, this->offt_root);
        if(type==BLOCK_INTER){
            return new CInternalNode(this->fpath,this->key_kind,this->max_key_size,this->offt_root);
        }
        else if (type==BLOCK_LEAF) {
            return new CLeafNode(this->fpath,this->key_kind,this->max_key_size,this->offt_root);
        }
        return NULL;
}
void BPlusTree::SetRoot(CNode* root)
{   // Sync update file
        m_Root = root;
        if( m_Root != NULL){
            m_Root->flush_file();
        this->offt_root = m_Root->getPtSelf();
        m_Root->setPtFather(INVALID);
        //cout<<"root offt"<<this->offt_root<<endl;
        }
        
}
bool BPlusTree::flush_file(){
    table t;
    memcpy(t.fpath, this->fpath, sizeof(this->fpath));

    t.offt_root = this->offt_root;
    t.offt_leftHead = this->offt_leftHead;
    t.offt_rightHead = this->offt_rightHead;
    t.key_use_block = this->key_use_block;
    t.value_use_block = this->value_use_block;
    t.key_kind = this->key_kind;
    t.m_Depth = this->m_Depth;
    t.max_key_size = this->max_key_size;
    t.attr_num = this->attr_num;
    for(int i=0;i<ATTR_MAX_NUM;i++){
        t.attr[i] = this->attr[i];
    }

    FileManager::getInstance()->flushTable(t, this->fpath, this->offt_self);
    return true;

}

bool BPlusTree::get_file() {
        table t = FileManager::getInstance()->getTable(this->fpath, this->offt_self);
        
        this->offt_root = t.offt_root;
        this->offt_leftHead = t.offt_leftHead;
        this->offt_rightHead = t.offt_rightHead;
        this->key_use_block = t.key_use_block;
        this->value_use_block = t.value_use_block;
        this->m_Depth = t.m_Depth;
        this->max_key_size = t.max_key_size;
        this->key_kind = t.key_kind;
        this->attr_num = t.attr_num;
        for(int i=0;i<this->attr_num;i++){
            this->attr[i] = t.attr[i];
            if(_stricmp(t.attr[i].constraint, "PRIMARY KEY")==0){
                strcpy(this->key_attr, t.attr[i].name);
            }
        }

        return true;
}

CLeafNode* BPlusTree::GetLeafHead(){
    if(this->m_pLeafHead!=NULL)return this->m_pLeafHead;
    char type = FileManager::getInstance()->get_BlockType(this->fpath, this->offt_leftHead);
    if(type==BLOCK_LEAF)return new CLeafNode(this->fpath, this->key_kind, this->max_key_size, this->offt_leftHead);
    return NULL;
}
CLeafNode* BPlusTree::GetLeafTail(){
    if(this->m_pLeafTail!=NULL)return this->m_pLeafTail;
    char type = FileManager::getInstance()->get_BlockType(this->fpath, this->offt_rightHead);
    if(type==BLOCK_LEAF)return new CLeafNode(this->fpath, this->key_kind, this->max_key_size, this->offt_rightHead);
    return NULL;
}
void BPlusTree::SetLeafHead(CLeafNode* node){
    this->m_pLeafHead = node;
    if(node!=NULL)this->offt_leftHead = node->getPtSelf();
}
void BPlusTree::SetLeafTail(CLeafNode* node){
    this->m_pLeafTail = node;
    if(node!=NULL)this->offt_rightHead = node->getPtSelf();
}
// Search data in tree
off_t BPlusTree::Search(void* data)
{
    int i = 0;
    int offset = 0;
    // if (NULL != sPath)
    // {
    //      (void)sprintf(sPath + offset, "The serach path is:");
    //      offset += 19;
    // }

    CNode* pNode = GetRoot();
    // Loop to find corresponding leaf node
    while (NULL != pNode)
    {
        // Node is leaf node, loop ends
        if (NODE_TYPE_LEAF == pNode->GetType())
        {
            break;
        }

        // Find first key value greater than or equal to key
        for (i = 1;  (i <= pNode->GetCount())&&(cmp(data , pNode->GetElement(i),this->key_kind)|| eql(data,pNode->GetElement(i),this->key_kind)); i++)
        {
        }

        // if (NULL != sPath)
        // {
        //      (void)sprintf(sPath + offset, " %3d -->", pNode->GetElement(1));
        //      offset += 8;
        // }

        pNode = pNode->GetPointer(i);
    }

    // Not found, there might be cases where pNode is null
    if (NULL == pNode)
    {
        return INVALID;
    }

    // if (NULL != sPath)
    // {
    //      (void)sprintf(sPath + offset, "%3d", pNode->GetElement(1));
    //      offset += 3;
    // }

    // Continue searching in leaf node
    off_t found = INVALID;
    for (i = 1; (i <= pNode->GetCount()); i++)
    {
        if (eql(data,pNode->GetElement(i),this->key_kind))
        {
            found = ((CLeafNode*)pNode)->GetElement_offt(i);
        }
    }

    // Release memory
    delete pNode;
    return found;
}

/* Insert data into B+ Tree
Inserting data first requires finding the theoretical leaf node for insertion, then there are three cases:
(1) Leaf node not full. Directly insert into the node;
(2) Leaf node full, and no father node (i.e., root node is leaf node). Need to split leaf node first, then choose to insert into original node or new node, then generate new root node;
(3) Leaf node full, but its father node not full. Need to split leaf node first, then choose to insert into original node or new node, then modify father node's pointer;
(4) Leaf node full, and its father node full. Need to split leaf node first, then choose to insert into original node or new node, then split father node, then modify grandfather node's pointer.
    Because grandfather node might also be full, it might need recursive splitting up to an ancestor node that is not full.
*/
off_t BPlusTree::Insert(void* data)  //
{
    // Check for duplicate insertion
    off_t found = Search(data);
    if (found!=INVALID)
    {
        return INVALID;
    }

    // Get a free block in file
    off_t offt_data = FileManager::getInstance()->getFreeBlock(this->fpath, BLOCK_DATA);

    // Find the ideal leaf node
    CLeafNode* pOldNode = SearchLeafNode(data);
    // If not found, tree is empty, generate root node
    if (NULL == pOldNode)
    {
        pOldNode = new CLeafNode(this->fpath, this->key_kind, this->max_key_size, NEW_OFFT);
        this->SetLeafHead(pOldNode);
        this->SetLeafTail(pOldNode);
        SetRoot(pOldNode);
    }

    // Leaf node not full, corresponding to case 1, insert directly
    if (pOldNode->GetCount() < MAXNUM_DATA)
    {
        bool success= pOldNode->Insert(data,offt_data);
        pOldNode->flush_file();
        if(pOldNode->getPtSelf()==this->offt_root)this->SetRoot(pOldNode);
        // Update data immediately after insertion
        //SetRoot(pOldNode);
        delete pOldNode;
        if(success)return offt_data;
        return INVALID;
    }

    // Original leaf node full, create new leaf node, and move the second half of data from original node to new node
    CLeafNode* pNewNode = new CLeafNode(this->fpath, this->key_kind, this->max_key_size, NEW_OFFT);
    void* key = INVALID;
    off_t offt_key = INVALID;
    key = pOldNode->Split(pNewNode);

    CLeafNode* pOldNext = pOldNode->GetNextNode();
    pOldNode->SetNextNode(pNewNode);
    pNewNode->SetNextNode(pOldNext);
    pNewNode->SetPrevNode(pOldNode);

    if (NULL == pOldNext)
    {
        //m_pLeafTail = pNewNode;
        this->SetLeafTail(pNewNode);
    }
    else
    {
        //pOldNext->m_pPrevNode = pNewNode;
        pOldNext->SetPrevNode(pNewNode);
    }


    // Determine whether to insert into original node or new node, ensuring sorting by data value
    if (cmp(key, data, this->key_kind))
    {
        pOldNode->Insert(data,offt_data);    // Insert into original node
    }
    else
    {
        pNewNode->Insert(data,offt_data);    // Insert into new node
    }

    // Father node
    CInternalNode* pFather = (CInternalNode*)(pOldNode->GetFather());

    // If original node is root node, corresponding to case 2
    if (NULL == pFather)
    {
        CNode* pNode1 = new CInternalNode(this->fpath, this->key_kind, this->max_key_size, NEW_OFFT);
        pNode1->SetPointer(1, pOldNode);                            // Pointer 1 points to original node
        pNode1->SetElement(1, key);                                // Set key
        pNode1->SetPointer(2, pNewNode);                            // Pointer 2 points to new node
        pOldNode->SetFather(pNode1);                               // Set father node
        pNewNode->SetFather(pNode1);                               // Set father node
        pNode1->SetCount(1);

        SetRoot(pNode1);
        delete pNode1;                                      // Set new root node
        pOldNode->flush_file();
        pNewNode->flush_file();
        delete pFather;
        delete pNewNode;
        delete pOldNode;
        delete pOldNext;
        return offt_data;
    }
    
    // Case 3 and Case 4 implemented here
    bool ret = InsertInternalNode(pFather, key, pNewNode);
    pFather->flush_file();
    pOldNode->flush_file();
    pNewNode->flush_file();
    pOldNode->flush_file();
    delete pFather;
    delete pNewNode;
    delete pOldNode;
    delete pOldNext;
    if(ret){
        return offt_data;
    }
    return INVALID;
}
bool BPlusTree::Insert_Data(vector<vector<string>> value){
    void* data[ATTR_MAX_NUM];
    // Find key value first
    void* key = INVALID;
    int key_index = -1;
    int key_index2=-1;
    for(int i=0;i<value[0].size();i++){
        if(_stricmp(this->key_attr, value[0][i].c_str())==0){
            key_index = i;
        }
    }
    for(int i=1;i<value.size();i++){
        for(int j=0;j<attr_num;j++){
            for(int z=0;z<value[i].size();z++){
                if(_stricmp(this->attr[j].name, value[0][z].c_str())==0){
                    data[j]=str2value(value[i][z].c_str(), this->attr[j].key_kind);
                    if(z==key_index)key_index2 = j;
                    
                    break;
                }
                if(z==value[i].size()-1){
                    data[j]=str2value("NULL", this->attr[j].key_kind);
                }
            }
        
       }
       if(key_index2!=-1){
           off_t offt_data = Insert(data[key_index2]);
            if(offt_data == INVALID){
                return false;
            }
            FileManager::getInstance()->flush_data(this->fpath, data, this->attr, this->attr_num, offt_data);
            }
    
    }

    //assign(key, data[key_index2], this->attr[key_index2].key_kind);
    
    return true;
}
void BPlusTree::Get_Data(void* data[ATTR_MAX_NUM],off_t offt){
    FileManager::getInstance()->get_data(this->fpath, data, this->attr, this->attr_num, offt);
}
void BPlusTree::Print_Data(void* data[ATTR_MAX_NUM]){
    for(int i=0;i<attr_num;i++){
        cout<<this->attr[i].name<<" ";
    }
    cout<<endl;
    for(int i=0;i<attr_num;i++){
        print_key(data[i], attr[i].key_kind);
    }
    cout<<endl;
}
/* Delete specific data
Algorithm for deleting data is as follows:
(1) If leaf node fill factor >= 50% after deletion, only need to modify leaf node. If key of father node is deleted, father node also needs modification;
(2) If leaf node fill factor < 50% after deletion, need to find a nearest brother node (left or right) first, then split into two cases:
    A. If the brother node fill factor > 50%, cut the nearest data of the brother node to this node, father node's key value also needs modification.
    B. If the brother node fill factor = 50%, combine two nodes, father node key also merged accordingly. (If father node fill factor < 50% after merge, recursion is needed)
*/
bool BPlusTree::Delete(void* data)
{
    // Find ideal leaf node
    CLeafNode* pOldNode = SearchLeafNode(data);
    // If not found, return failure
    if (NULL == pOldNode)
    {
        return false;
    }

    // Delete data, if failed it must be not found, return failure directly
    bool success = pOldNode->Delete(data,true);
    pOldNode->flush_file();
    if (false == success)
    {
        return false;
    }

    // Get father node
    CInternalNode* pFather = (CInternalNode*)(pOldNode->GetFather());
    if (NULL == pFather)
    {
        pOldNode->flush_file();
        // If no data left, delete root node (only root node can have this situation)
        if (0 == pOldNode->GetCount())
        {
            
            pOldNode->FreeBlock();
            this->SetLeafHead(NULL);
            this->SetLeafTail(NULL);
            //m_pLeafHead = NULL;
            //m_pLeafTail = NULL;
            SetRoot(NULL);
        }
        delete pOldNode;
        return true;
    }


    // If leaf node fill factor >= 50% after deletion, corresponding to case 1
    if (pOldNode->GetCount() >= ORDER_V)
    {
        for (int i = 1; (i <= pFather->GetCount())&& (cmp(data , pFather->GetElement(i),this->key_kind)||eql(data, pFather->GetElement(i), this->key_kind)) ; i++)
        {
            // If father node's key value is deleted, need to change that key
            if (eql(pFather->GetElement(i) ,data,this->key_kind))
            {
                pFather->SetElement(i, pOldNode->GetElement(1));    // Change to new first element of leaf node
            }
        }
        updateNode(pFather);
        updateNode(pOldNode);
        return true;
    }

    // Find a nearest brother node (according to B+ tree definition, always can be found except for leaf node)
    int flag = FLAG_LEFT;
    CLeafNode* pBrother = (CLeafNode*)(pOldNode->GetBrother(flag));
    // Here pBrother might be null due to special reasons like father node not found, so need to check
    if (pBrother == NULL){
        // Fix correct father node
        bool b=this->SetCorrentFather(pOldNode);
        if(b)CLeafNode* pBrother = (CLeafNode*)(pOldNode->GetBrother(flag));
    }
    if(NULL == pBrother)
    {
        return true;// Nothing can be done, cannot propagate upwards.
    }
    // Brother node fill factor > 50%, corresponding to case 2A
    void* NewData = INVALID;
    off_t NewDataPos = 0;
    if (pBrother->GetCount() > ORDER_V)
    {
        if (FLAG_LEFT == flag)    // Brother on left, move last data here
        {
            NewData = pBrother->GetElement(pBrother->GetCount());
            NewDataPos = pBrother->GetElement_offt( pBrother->GetCount());
        }
        else    // Brother on right, move first data here
        {
            NewData = pBrother->GetElement(1);
            NewDataPos = pBrother->GetElement_offt(1);
        }

        pOldNode->Insert(NewData, NewDataPos);
        pBrother->Delete(NewData,false);
        pOldNode->flush_file();
        pBrother->flush_file();
        // Modify father node's key value
        if (FLAG_LEFT == flag)
        {
            for (int i = 1; i <= pFather->GetCount() + 1; i++)
            {
                if (pFather->GetPointer(i)->getPtSelf()== pOldNode->getPtSelf()&&i > 1)
                {
                    pFather->SetElement(i - 1, pOldNode->GetElement(1));    // Change key corresponding to this node
                }
            }
        }
        else
        {
            for (int i = 1; i <= pFather->GetCount() + 1; i++)
            {
                if (pFather->GetPointer(i)->getPtSelf()==pOldNode->getPtSelf() &&i > 1)
                {
                    pFather->SetElement(i - 1, pOldNode->GetElement(1));    // Change key corresponding to this node
                }
                if (pFather->GetPointer(i)->getPtSelf()==pBrother->getPtSelf() && i > 1)
                {
                    pFather->SetElement(i - 1, pBrother->GetElement(1));    // Change key corresponding to brother node
                }
            }
        }

        updateNode(pFather);
        updateNode(pOldNode);
        updateNode(pBrother);
        return true;
    }

    // Case 2B

    // Key to delete in father node
    void* NewKey = NULL;

    // Combine this node with brother node, combine into the node with smaller data anyway, so father node doesn't need to modify pointer

    if (FLAG_LEFT == flag)
    {
        pBrother->Combine(pOldNode);
        NewKey = pOldNode->GetElement(1);
        //print_key(NewKey, this->key_kind);
        CLeafNode* pOldNext = pOldNode->GetNextNode();
        pBrother->SetPrevNode(pOldNext);
        //pBrother->m_pNextNode = pOldNext;
        // Delete node in doubly linked list
        if (NULL == pOldNext)
        {
            this->SetLeafTail(pBrother);
            //m_pLeafTail = pBrother;
        }
        else
        {
            pOldNext->SetPrevNode(pBrother);
            //pOldNext->m_pPrevNode = pBrother;
        }
        // Delete this node
        pBrother->flush_file();
        delete pOldNode;
    }
    else
    {
        pOldNode->Combine(pBrother);
        NewKey = pBrother->GetElement(1);

        CLeafNode* pOldNext = pBrother->GetNextNode();
        pOldNode->SetNextNode(pOldNext);
        //pOldNode->m_pNextNode = pOldNext;
        // Delete node in doubly linked list
        if (NULL == pOldNext)
        {
            this->SetLeafTail(pOldNode);
            //m_pLeafTail = pOldNode;
        }
        else
        {
            pOldNext->SetPrevNode(pOldNode);
            //pOldNext->m_pPrevNode = pOldNode;
        }
        // Delete this node
        pOldNode->flush_file();
        delete pBrother;
    }

    return DeleteInternalNode(pFather, NewKey);
}

// Clear the whole tree, delete all nodes
void BPlusTree::ClearTree()
{
    CNode* pNode = GetRoot();
    if (NULL != pNode)
    {
        pNode->DeleteChildren();

        delete pNode;
    }

    m_pLeafHead = NULL;
    m_pLeafTail = NULL;
    SetRoot(NULL);
}

// Rotate to re-balance, actually reconstructs the whole tree, result not ideal, need reconsideration
BPlusTree* BPlusTree::RotateTree()
{
    BPlusTree* pNewTree = new BPlusTree;
    int i = 0;
    CLeafNode* pNode = m_pLeafHead;
    while (NULL != pNode)
    {
        for (int i = 1; i <= pNode->GetCount(); i++)
        {
            (void)pNewTree->Insert(pNode->GetElement(i));
        }

        pNode = pNode->m_pNextNode;
    }

    return pNewTree;

}
// Check if tree satisfies B+ tree definition
bool BPlusTree::CheckTree()
{
    CLeafNode* pThisNode = m_pLeafHead;
    CLeafNode* pNextNode = NULL;
    while (NULL != pThisNode)
    {
        pNextNode = pThisNode->m_pNextNode;
        if (NULL != pNextNode)
        {
            if (pThisNode->GetElement(pThisNode->GetCount()) > pNextNode->GetElement(1))
            {
                return false;
            }
        }
        pThisNode = pNextNode;
    }

    return CheckNode(GetRoot());
}

// Recursively check if node and its subtrees satisfy B+ tree definition
bool BPlusTree::CheckNode(CNode* pNode)
{
    if (NULL == pNode)
    {
        return true;
    }

    int i = 0;
    bool ret = false;

    // Check if 50% fill factor is satisfied
    if ((pNode->GetCount() < ORDER_V) && (pNode != GetRoot()))
    {
        return false;
    }

    // Check if key or data are sorted
    for (i = 1; i < pNode->GetCount(); i++)
    {
        if (pNode->GetElement(i) > pNode->GetElement(i + 1))
        {
            return false;
        }
    }

    if (NODE_TYPE_LEAF == pNode->GetType())
    {
        return true;
    }

    // For internal nodes, recursively check subtrees
    for (i = 1; i <= pNode->GetCount() + 1; i++)
    {
        ret = CheckNode(pNode->GetPointer(i));
        // If any one is invalid, return invalid
        if (false == ret)
        {
            return false;
        }
    }

    return true;

}

// Print the whole tree, used during initial B+ tree usage, useless later
void BPlusTree::PrintTree()
{
    CNode* pRoot = GetRoot();
    if (NULL == pRoot) return;

    CNode* p1, * p2, * p3;
    int i, j, k;
    int total = 0;

    printf("\nLevel 1\n | ");
    PrintNode(pRoot);
    total = 0;
    printf("\nLevel 2\n | ");
    for (i = 1; i <= MAXNUM_POINTER; i++)
    {
        p1 = pRoot->GetPointer(i);
        if (NULL == p1) continue;
        PrintNode(p1);
        total++;
        if (total % 4 == 0) printf("\n | ");
    }
    total = 0;
    printf("\nLevel 3\n | ");
    for (i = 1; i <= MAXNUM_POINTER; i++)
    {
        p1 = pRoot->GetPointer(i);
        if (NULL == p1) continue;
        for (j = 1; j <= MAXNUM_POINTER; j++)
        {
            p2 = p1->GetPointer(j);
            if (NULL == p2) continue;
            PrintNode(p2);
            total++;
            if (total % 4 == 0) printf("\n | ");
        }
    }
    total = 0;
    printf("\nLevel 4\n | ");
    for (i = 1; i <= MAXNUM_POINTER; i++)
    {
        p1 = pRoot->GetPointer(i);
        if (NULL == p1) continue;
        for (j = 1; j <= MAXNUM_POINTER; j++)
        {
            p2 = p1->GetPointer(j);
            if (NULL == p2) continue;
            for (k = 1; k <= MAXNUM_POINTER; k++)
            {
                p3 = p2->GetPointer(k);
                if (NULL == p3) continue;
                PrintNode(p3);
                total++;
                if (total % 4 == 0) printf("\n | ");
            }
        }
    }
}

// Print a node, used for testing
void BPlusTree::PrintNode(CNode* pNode)
{
    if (NULL == pNode)
    {
        return;
    }

    for (int i = 1; i <= MAXNUM_KEY; i++)
    {
        if(this->key_kind==INT_KEY){
            cout<<*(int*)pNode->GetElement(i)<<" ";
        }
        else if (this->key_kind==LL_KEY) {
            cout<<*(long long*)pNode->GetElement(i)<<" ";
        }
        else{
            cout<<(char*)pNode->GetElement(i)<<" ";
        }
        
        if (i >= MAXNUM_KEY)
        {
            printf(" | ");
        }
    }
}

// Search for corresponding leaf node
CLeafNode* BPlusTree::SearchLeafNode(void* data)
{
    int i = 0;

    CNode* pNode = GetRoot();
    // Loop to find corresponding leaf node
    while (NULL != pNode)
    {
        // Node is leaf node, loop ends
        if (NODE_TYPE_LEAF == pNode->GetType())
        {
            break;
        }

        // Find first key value greater than or equal to key
        for (i = 1; i <= pNode->GetCount(); i++)
        {
            
            if (cmp(pNode->GetElement(i), data,this->key_kind))
            {
                break;
            }
        }

        pNode = pNode->GetPointer(i);
    }

    return (CLeafNode*)pNode;
}

bool BPlusTree::SetCorrentFather(CLeafNode* leaf){
    int i = 0;

    CNode* pNode = GetRoot();
    CNode* p1 = NULL;
    // Loop to find corresponding leaf node, really afraid of infinite loop later
    while (pNode->GetType()!=NODE_TYPE_LEAF)
    {
        
        // Find first key value greater than or equal to key
        for (i = 1; i <= pNode->GetCount(); i++)
        {
            if (cmp(pNode->GetElement(i), leaf->GetElement(1),this->key_kind))
            {
                break;
            }
        }
        delete p1;
        p1 = new CInternalNode(this->fpath, this->key_kind, this->max_key_size, pNode->getPtSelf());
        pNode = pNode->GetPointer(i);
    }
    if(i!=0&&p1!=NULL&&p1->GetType()==NODE_TYPE_INTERNAL){
        leaf->SetFather(p1);
        leaf->flush_file();
        delete p1;
        return true;
    }
    else{
        return false;
    }
    
}

bool BPlusTree::SetCorrentFather(CInternalNode* node){
    int i = 0;

    CNode* pNode = GetRoot();
    CNode* p1 = NULL;
    // Loop to find the corresponding leaf node, really afraid of infinite loop later
    while (pNode->getPtSelf()!=node->getPtSelf())
    {
        
        // Find the first key position greater than or equal to key
        for (i = 1; i <= pNode->GetCount(); i++)
        {
            if (cmp(pNode->GetElement(i), node->GetElement(1),this->key_kind))
            {
                break;
            }
        }
        if(pNode->getPtSelf()==node->getPtSelf())break;
        delete p1;
        p1 = new CInternalNode(this->fpath, this->key_kind, this->max_key_size, pNode->getPtSelf());
        pNode = pNode->GetPointer(i);
    }
    if(i!=0&&p1!=NULL&&p1->GetType()==NODE_TYPE_INTERNAL){
        node->SetFather(p1);
        node->flush_file();
        delete p1;
        return true;
    }
    else{
        return false;
    }
}

// Recursive function: Insert key into internal node
bool BPlusTree::InsertInternalNode(CInternalNode* pNode, void* key, CNode* pRightSon)
{
    if (NULL == pNode || NODE_TYPE_LEAF == pNode->GetType())
    {
        return false;
    }

    // Node is not full, insert directly
    if (pNode->GetCount() < MAXNUM_KEY)
    {
        bool ans=pNode->Insert(key, pRightSon);
        pNode->flush_file();
        return ans;
    }

    CInternalNode* pBrother = new CInternalNode(this->fpath, this->key_kind, this->max_key_size, NEW_OFFT);  // In C++, new ClassName allocates memory space required for a class and returns its starting address
    void* NewKey = INVALID;
    // Split this node
    NewKey = pNode->Split(pBrother, key);
    //print_key(NewKey, this->key_kind);
    if (pNode->GetCount() < pBrother->GetCount())
    {
        pNode->Insert(key, pRightSon);
    }
    else if (pNode->GetCount() > pBrother->GetCount())
    {
        pBrother->Insert(key, pRightSon);
    }
    else    // Both are equal, i.e., the key is between the V-th and (V+1)-th keys, attach the child node to the first pointer of the new node
    {
        pBrother->SetPointer(1, pRightSon);
        pRightSon->SetFather(pBrother);
    }
    pRightSon->flush_file();
    CInternalNode* pFather = (CInternalNode*)(pNode->GetFather());
    // Until root node is full, generate new root node
    if (NULL == pFather)
    {
        pFather = new CInternalNode(this->fpath, this->key_kind, this->max_key_size, NEW_OFFT);
        pFather->SetPointer(1, pNode);        // Pointer 1 points to original node
        pFather->SetElement(1, NewKey);       // Set key
        pFather->SetPointer(2, pBrother);     // Pointer 2 points to new node
        pNode->SetFather(pFather);            // Specify father node
        pBrother->SetFather(pFather);         // Specify father node
        pFather->SetCount(1);

        pBrother->flush_file();
        pNode->flush_file();
        SetRoot(pFather);
        delete pBrother;
        delete pFather;
        return true;
    }
    pNode->flush_file();
    // Recursion
    return InsertInternalNode(pFather, NewKey, pBrother);
}

// Recursive function: Delete key in internal node
bool BPlusTree::DeleteInternalNode(CInternalNode* pNode, void* key)
{
    // Delete key, if failed it must be not found, return failure directly
    bool success = pNode->Delete(key);
    
    if (false == success)
    {
        return false;
    }

    // Get father node
    CInternalNode* pFather = (CInternalNode*)(pNode->GetFather());
    if (NULL == pFather)
    {
        // If no data left, make the first node of the root node the new root
        if (0 == pNode->GetCount())
        {
            SetRoot(pNode->GetPointer(1));
            pNode->FreeBlock(); // Free node
            delete pNode;
        }
        else SetRoot(pNode);

        return true;
    }

    // Fill factor still >= 50% after deletion
    if (pNode->GetCount() >= ORDER_V)
    {
        for (int i = 1; (i <= pFather->GetCount())&&(cmp(key , pFather->GetElement(i),this->key_kind) || eql(key, pFather->GetElement(i), this->key_kind)) ; i++)
        {
            // If the deleted key is from the father node, need to change that key
            if (eql(pFather->GetElement(i) , key,this->key_kind))
            {
                CNode* node= pNode->GetPointer(i+1);
                while(node->GetType()!=NODE_TYPE_LEAF)node=node->GetPointer(1);
                pFather->SetElement(i, node->GetElement(1));    // Change to the new first element of the leaf node
                pFather->flush_file();
            }
        }
        delete pFather;
        return true;
    }

    // Find a nearest brother node (according to B+ tree definition, always found except for root node)
    int flag = FLAG_LEFT;
    CInternalNode* pBrother = (CInternalNode*)(pNode->GetBrother(flag));
    // Handle case where brother node is not found (fix processing)
    if (NULL == pBrother)
    {
        bool a=this->SetCorrentFather(pNode);
        if(a)pBrother = (CInternalNode*)(pNode->GetBrother(flag));
        
    }
    if(pBrother==NULL)return true;// Return helplessly
    // Brother node fill factor > 50%
    void* NewData = INVALID;
    if (pBrother->GetCount() > ORDER_V)
    {
        pNode->MoveOneElement(pBrother);
        // Modify key value of father node
        if (FLAG_LEFT == flag)
        {
            for (int i = 1; i <= pFather->GetCount() + 1; i++)
            {
                // if (pFather->GetPointer(i)->getPtSelf() == pNode->getPtSelf() && i > 1)
                // {
                //      pFather->SetElement(i - 1, pNode->GetElement(1));    // Change key corresponding to this node
                // }
                 if (pFather->GetPointer(i)->getPtSelf() == pNode->getPtSelf() && i > 1)
                {
                    CNode* node= pNode->GetPointer(1);
                    while(node->GetType()!=NODE_TYPE_LEAF)node=node->GetPointer(1);
                    pFather->SetElement(i - 1, node->GetElement(1));    // Change key corresponding to this node
                }
            }
        }
        else
        {
            for (int i = 1; i <= pFather->GetCount() + 1; i++)
            {
                // if (pFather->GetPointer(i)->getPtSelf() == pNode->getPtSelf() && i > 1)
                // {
                //      pFather->SetElement(i - 1, pNode->GetElement(1));    // Change key corresponding to this node
                // }
                // if (pFather->GetPointer(i)->getPtSelf() == pBrother->getPtSelf() && i > 1)
                // {
                //      pFather->SetElement(i - 1, pBrother->GetElement(1));    // Change key corresponding to brother node
                // }
                if (pFather->GetPointer(i)->getPtSelf() == pNode->getPtSelf() && i > 1)
                {
                    CNode* node= pNode->GetPointer(1);
                    while(node->GetType()!=NODE_TYPE_LEAF)node=node->GetPointer(1);
                    pFather->SetElement(i - 1, node->GetElement(1));    // Change key corresponding to this node
                    //pFather->SetElement(i - 1, pNode->GetPointer(1)->GetElement(1));    // Change key corresponding to this node
                }
                if (pFather->GetPointer(i)->getPtSelf() == pBrother->getPtSelf() && i > 1)
                {
                    CNode* node= pBrother->GetPointer(1);
                    while(node->GetType()!=NODE_TYPE_LEAF)node=node->GetPointer(1);
                    pFather->SetElement(i - 1, node->GetElement(1));    // Change key corresponding to brother node
                }
            }
        }
        updateNode(pFather);
        updateNode(pBrother);
        pNode->flush_file();
        return true;
    }

    // Key to delete in father node: brother nodes are not > 50%, need to merge nodes, father node needs to delete key
    void* NewKey = NULL;

    // Merge this node with brother node, strictly merge into the node with smaller data, so father node pointer doesn't need modification
    if (FLAG_LEFT == flag)
    {
        pBrother->Combine(pNode);
        NewKey = pNode->GetElement(1);
        pBrother->flush_file();
        pNode->FreeBlock();
        delete pNode;
    }
    else
    {
        pNode->Combine(pBrother);
        NewKey = pBrother->GetElement(1);
        pNode->flush_file();
        //pBrother->FreeBlock();
        delete pBrother;
    }

    // Recursion
    return DeleteInternalNode(pFather, NewKey);
}
void BPlusTree::Select_Data(vector<string>attributenames,vector<LOGIC>Logics,vector<WhereCondition>w){
    // First write the detection only for id
    if(w.size()==1&&_stricmp(w[0].attribute.c_str(),this->key_attr)==0&&w[0].operatorSymbol=="="){
        void* key;
        if(w[0].operatorSymbol=="="){
            key=str2value(w[0].value, this->key_kind);
        }
        off_t offt_data=this->Search(key);
        if(offt_data==INVALID){
            this->Print_Header(attributenames);
            return;
        }
        void* data[ATTR_MAX_NUM];
        this->Get_Data(data, offt_data);
        this->Print_Header(attributenames);
        this->Print_Data(data,attributenames);
        return;
    }
    // Write other cases after finishing traversal
    this->Print_Header(attributenames);
    for(int i=0;i<NUM_ALL_BLOCK;i++){
        if(this->Block_GRAPH[i]==BLOCK_DATA){
            void* data[ATTR_MAX_NUM];
            this->Get_Data(data, i);
            if(this->SatisfyConditions(w, Logics, data))this->Print_Data(data,attributenames);
        }
    }

}

void BPlusTree::Print_Data(void* data[ATTR_MAX_NUM],vector<string>attributenames){
    if(attributenames[0]=="*"){
        for(int i=0;i<this->attr_num;i++){
            print_key(data[i],this->attr[i].key_kind );
        }
        cout<<endl;
        return;
    }
    vector<int>index;
    for(int i=0;i<attributenames.size();i++){
        for(int j=0;j<this->attr_num;j++){
            if(_stricmp(attributenames[i].c_str(),this->attr[j].name)==0){
                index.push_back(j);
                break;
            }
            if(j==this->attr_num-1){
                cout<<"error:attribute not found"<<endl;
                return;
            }
        }
    }
    if(attributenames.size()!=index.size()){
        cout<<"error:attribute not found"<<endl;
        return;

    }
    for(int i=0;i<attributenames.size();i++)
        print_key(data[index[i]],this->attr[index[i]].key_kind );
    cout<<endl;
}
void BPlusTree::Print_Header(vector<string>attributenames){
    if(attributenames[0]=="*"){
        for(int i=0;i<this->attr_num;i++){
            cout<<'|'<<this->attr[i].name<<'\t';
        }
        cout<<endl;
        return;
    }

    vector<int>index;
    for(int i=0;i<attributenames.size();i++){
        for(int j=0;j<this->attr_num;j++){
            if(_stricmp(attributenames[i].c_str(),this->attr[j].name)==0){
                index.push_back(j);
                break;
            }
            if(j==this->attr_num-1){
                cout<<"error:attribute not found"<<endl;
                return;
            }
        }
    }
    if(attributenames.size()!=index.size()){
        cout<<"error:attribute not found"<<endl;
        return;

    }
    for(int i=0;i<attributenames.size();i++){
        cout<<'|'<<attributenames[i]<<'\t';
    }
    cout<<endl;
}

void BPlusTree::Select_Data_Join(vector<string>attributenames, vector<LOGIC>Logics, vector<WhereCondition>w) {
    if (this->joinBp == NULL) {
        cout << "Cannot open joined table" << endl;
        return;
    }

    // [新增] Helper lambda: 去除表名前缀 (例如 "table.col" -> "col")
    // 这一步至关重要，用于解决 SQL 解析出的全名与存储层短名不一致的问题
    auto removeTablePrefix = [](string col) -> string {
        size_t pos = col.find('.');
        if (pos != string::npos) {
            return col.substr(pos + 1);
        }
        return col;
    };

    // Dynamically find join keys
    string local_join_key = ""; // Join column of local table (e.g., dept_id)
    string remote_join_key = ""; // Join column of remote table (e.g., did)

    // If foreign key is not found in constructor, look for it in WHERE conditions
    if (this->foreign_key == "") {
        for (auto& cond : w) {
            if (cond.operatorSymbol != "=") continue;

            // Check if attribute is in current table and value is in join table
            bool attrInLocal = false;
            bool valInRemote = false;

            // [修改] 使用去除前缀后的纯列名进行比对
            string pureAttr = removeTablePrefix(cond.attribute);
            string pureVal = removeTablePrefix(cond.value);

            for (int i = 0; i < this->attr_num; i++) {
#ifdef _WIN32
                if (_stricmp(this->attr[i].name, pureAttr.c_str()) == 0) attrInLocal = true;
#else
                if (strcasecmp(this->attr[i].name, pureAttr.c_str()) == 0) attrInLocal = true;
#endif
            }

            for (int i = 0; i < this->joinBp->attr_num; i++) {
#ifdef _WIN32
                if (_stricmp(this->joinBp->attr[i].name, pureVal.c_str()) == 0) valInRemote = true;
#else
                if (strcasecmp(this->joinBp->attr[i].name, pureVal.c_str()) == 0) valInRemote = true;
#endif
            }

            if (attrInLocal && valInRemote) {
                local_join_key = cond.attribute;
                remote_join_key = cond.value; // Here value is parsed as column name "did"
                this->foreign_key = remote_join_key; // Set member variable for later use
                break;
            }
        }

        if (this->foreign_key == "") {
            cout << "Error: Valid join condition (Table1.Col = Table2.Col) not found in WHERE clause" << endl;
            return;
        }
    }
    else {
        // If explicit foreign key exists, we need to reverse lookup the local column name
        remote_join_key = this->foreign_key;
        for (int i = 0; i < attr_num; i++) {
            string t = this->attr[i].constraint;
            if (t.find(this->ref_table) != string::npos) {
                local_join_key = this->attr[i].name;
                break;
            }
        }
    }

    this->Print_Header_Join(attributenames);

    vector<WhereCondition> wc1;
    vector<WhereCondition> wc2;
    int local_join_index = -1;

    // [修改] 查找本地 Join 列索引时，也要去除前缀
    string pureLocalKey = removeTablePrefix(local_join_key);

    // Find index of local join column
    for (int i = 0; i < this->attr_num; i++) {
#ifdef _WIN32
        if (_stricmp(this->attr[i].name, pureLocalKey.c_str()) == 0)
#else
        if (strcasecmp(this->attr[i].name, pureLocalKey.c_str()) == 0)
#endif
        {
            local_join_index = i;
            break;
        }
    }

    // Separate conditions: WC1 for main table, WC2 for joined table
    for (int i = 0; i < w.size(); i++) {
        // Ignore join condition itself
        // [修改] 比较时两边都剥离前缀，确保匹配准确
        if (removeTablePrefix(w[i].attribute) == removeTablePrefix(local_join_key) && 
            removeTablePrefix(w[i].value) == removeTablePrefix(remote_join_key)) {
            continue;
        }

        bool isLocal = false;
        // [修改] 剥离前缀后判断归属
        string currentAttr = removeTablePrefix(w[i].attribute);

        for (int j = 0; j < this->attr_num; j++) {
#ifdef _WIN32
            if (_stricmp(currentAttr.c_str(), this->attr[j].name) == 0)
#else
            if (strcasecmp(currentAttr.c_str(), this->attr[j].name) == 0)
#endif
            {
                isLocal = true;
                break;
            }
        }

        // [重要修改] 必须创建一个新的 WhereCondition 并修改 attribute 为剥离前缀后的版本
        // 否则底层的 SatisfyCondition 拿 "employee.age" 去比对 "age" 会失败
        WhereCondition cleanCond = w[i];
        cleanCond.attribute = currentAttr; 

        if (isLocal) {
            wc1.push_back(cleanCond);
        }
        else {
            // Assume belongs to table 2, also strip prefix
            cleanCond.attribute = removeTablePrefix(w[i].attribute);
            wc2.push_back(cleanCond);
        }
    }

    // Execute Nested Loop Join
    for (int i = 0; i < NUM_ALL_BLOCK; i++) {
        if (this->Block_GRAPH[i] == BLOCK_DATA) {
            void* data[ATTR_MAX_NUM];
            this->Get_Data(data, i);

            // 1. Check main table conditions
            if (this->SatisfyConditions(wc1, Logics, data)) {
                
                // 2. Construct query condition for joined table: RemoteKey = LocalValue
                vector<WhereCondition> wc_join = wc2;
                string joinVal = value2str(data[local_join_index], this->attr[local_join_index].key_kind);

                // The value here must be a concrete value, not a column name
                // [修改] Remote Key 也需要去除前缀
                WhereCondition joinCond(removeTablePrefix(remote_join_key), "=", joinVal);
                wc_join.push_back(joinCond);

                vector<LOGIC> logic_join;
                for (size_t k = 0; k < wc_join.size(); k++) logic_join.push_back(AND_LOGIC);

                // 3. Scan joined table
                for (int j = 0; j < NUM_ALL_BLOCK; j++) {
                    if (this->joinBp->Block_GRAPH[j] == BLOCK_DATA) {
                        void* data2[ATTR_MAX_NUM];
                        this->joinBp->Get_Data(data2, j);

                        if (this->joinBp->SatisfyConditions(wc_join, logic_join, data2)) {
                            this->Print_Data_Join(data, data2, attributenames);
                        }
                    }
                }
            }
        }
    }
}

void BPlusTree::Print_Header_Join(vector<string>attributenames){
    
    if(attributenames[0]=="*"){
        for(int i=0;i<this->attr_num;i++){
            cout<<'|'<<this->attr[i].name<<'\t';
        }
        for(int i=0;i<this->joinBp->attr_num;i++){
            if(_stricmp(this->joinBp->attr[i].name, this->foreign_key.c_str())==0)continue;
            cout<<'|'<<this->joinBp->attr[i].name<<'\t';
        }
        cout<<endl;
        return;
    }
    vector<int>index1;
    vector<int>index2;
    for(int i=0;i<attributenames.size();i++){
        for(int j=0;j<this->attr_num;j++){
            if(_stricmp(attributenames[i].c_str(),this->attr[j].name)==0){
                index1.push_back(j);
                break;
            }
        }
        for(int j=0;j<this->joinBp->attr_num;j++){
            if(_stricmp(attributenames[i].c_str(),this->joinBp->attr[j].name)==0&&_stricmp(this->joinBp->attr[j].name, this->foreign_key.c_str())!=0){
                index2.push_back(j);
                break;
        }
    }
    }
    for(int i=0;i<index1.size();i++){
        cout<<'|'<<this->attr[index1[i]].name<<'\t';
    }
    for(int i=0;i<index2.size();i++){
        cout<<'|'<<this->joinBp->attr[index2[i]].name<<'\t';
    }
    cout<<endl;
}

void BPlusTree::Print_Data_Join(void* data1[ATTR_MAX_NUM],void* data2[ATTR_MAX_NUM],vector<string>attributenames){
    if(attributenames[0]=="*"){
        for(int i=0;i<this->attr_num;i++){
            print_key(data1[i],this->attr[i].key_kind );
        }
        for(int i=0;i<this->joinBp->attr_num;i++){
            if(_stricmp(this->joinBp->attr[i].name, this->foreign_key.c_str())==0)continue;
            print_key(data2[i],this->joinBp->attr[i].key_kind );
        }
        cout<<endl;
        return;
    }
    vector<int>index1;
    vector<int>index2;
    for(int i=0;i<attributenames.size();i++){
        for(int j=0;j<this->attr_num;j++){
            if(_stricmp(attributenames[i].c_str(),this->attr[j].name)==0){
                index1.push_back(j);
                break;
            }
        }
        for(int j=0;j<this->joinBp->attr_num;j++){
            if(_stricmp(attributenames[i].c_str(),this->joinBp->attr[j].name)==0&&_stricmp(this->joinBp->attr[j].name, this->foreign_key.c_str())!=0){
                index2.push_back(j);
        }
    }
    }
    
    for(int i=0;i<index1.size();i++)print_key(data1[index1[i]],this->attr[index1[i]].key_kind );
    for(int i=0;i<index2.size();i++)print_key(data2[index2[i]],this->joinBp->attr[index2[i]].key_kind );
    cout<<endl;
}

bool BPlusTree::SatisfyCondition(WhereCondition w,void* data[ATTR_MAX_NUM]){
    
    void* compare_key;
    int j=-1;
    for(int i=0;i<this->attr_num;i++){
        if(_stricmp(w.attribute.c_str(),this->attr[i].name)==0){
            compare_key=str2value(w.value, this->attr[i].key_kind);
            j=i;
            break;
        }
    }
    if(j==-1){
        cout<<"false condition"<<endl;
        return false;
    }
    if(w.operatorSymbol[0]=='='){
        if(eql(data[j],compare_key,this->attr[j].key_kind))return true;
        return false;
    }
    else if(w.operatorSymbol[0]=='<'){
        if(cmp(data[j],compare_key,this->attr[j].key_kind))return false;
        
        if(w.operatorSymbol.size()==1
            &&eql(data[j],compare_key,this->attr[j].key_kind))return false;

        return true;
    }
    else if(w.operatorSymbol[0]=='>'){
        if(cmp(compare_key,data[j],this->attr[j].key_kind))return false;
        if(w.operatorSymbol.size()==1
            &&eql(data[j],compare_key,this->attr[j].key_kind))return false;

        return true;
        }

    return true;
}

bool BPlusTree::SatisfyConditions(vector<WhereCondition>w,vector<LOGIC>Logics,void* data[ATTR_MAX_NUM]){
    bool flag=true;
    if(w.size()==0)return true;
    bool flag1=SatisfyCondition(w[0],data);
    if(Logics.size()==0)return flag1;
    for(int i=1;i<w.size();i++){
        if(Logics[i-1]==AND_LOGIC){
            flag=SatisfyCondition(w[i],data);
            flag1=flag1&&flag;
        }
        else if(Logics[i-1]==OR_LOGIC){
            flag=SatisfyCondition(w[i],data);
            if(flag1||flag)return true;
            flag1=false;
        }
    }
    return flag1;
}

bool BPlusTree::Delete_Data(vector<WhereCondition>w,vector<LOGIC>Logics){
    if(w.size()==0){
        // delete the whole tree
        FileManager::getInstance()->deleteFile(this->fpath);
        return true;
    }
    if(w.size()==1&&_stricmp(w[0].attribute.c_str(), this->key_attr)==0&&w[0].operatorSymbol=="="){
        void* key=str2value(w[0].value, this->key_kind);
        return this->Delete(key);
    }
    int key_index=-1;
    for(int i=0;i<this->attr_num;i++){
        if(_stricmp(this->key_attr,this->attr[i].name)==0){
            key_index=i;
            break;
        }
    }
    if(key_index==-1)return false;
    for(int i=0;i<NUM_ALL_BLOCK;i++){
        if(this->Block_GRAPH[i]==BLOCK_DATA){
            void* data[ATTR_MAX_NUM];
            this->Get_Data(data, i);
            if(SatisfyConditions(w,Logics,data)){
                this->Delete(data[key_index]);
            }
        }
    }
    return true;
    
}

bool BPlusTree::Update_Data(vector<WhereCondition> whereConditions, vector<WhereCondition> setAttributes) {
    if (setAttributes.empty()) {
        cout << "Error: No attributes specified for update" << endl;
        return false;
    }
    
    int update_count = 0;
    
    // Simple implementation: Only handle the case where the root node is a leaf
    if (m_Root && m_Root->GetType() == NODE_TYPE_LEAF) {
        CLeafNode* pLeaf = (CLeafNode*)m_Root;
        
        for (int i = 1; i <= pLeaf->GetCount(); i++) {
            off_t data_offset = pLeaf->GetElement_offt(i);
            if (data_offset == 0 || data_offset == INVALID) continue;
            
            void* data[ATTR_MAX_NUM];
            this->Get_Data(data, data_offset);
            
            // Check WHERE conditions
            bool satisfy = true;
            for (auto& w : whereConditions) {
                for (int j = 0; j < this->attr_num; j++) {
                    #ifdef _WIN32
                        if (_stricmp(w.attribute.c_str(), this->attr[j].name) == 0)
                    #else
                        if (strcasecmp(w.attribute.c_str(), this->attr[j].name) == 0)
                    #endif
                    {
                        void* cmp_val = str2value(w.value, this->attr[j].key_kind);
                        if (w.operatorSymbol == "=" && !eql(data[j], cmp_val, this->attr[j].key_kind)) {
                            satisfy = false;
                        }
                        break;
                    }
                }
            }
            
            if (satisfy) {
                // Update attributes
                for (auto& s : setAttributes) {
                    for (int k = 0; k < this->attr_num; k++) {
                        #ifdef _WIN32
                            if (_stricmp(s.attribute.c_str(), this->attr[k].name) == 0)
                        #else
                            if (strcasecmp(s.attribute.c_str(), this->attr[k].name) == 0)
                        #endif
                        {
                            data[k] = str2value(s.value, this->attr[k].key_kind);
                            break;
                        }
                    }
                }
                
                FileManager::getInstance()->flush_data(this->fpath, data, this->attr, this->attr_num, data_offset);
                update_count++;
            }
        }
    }
    
    cout << "Updated " << update_count << " records" << endl;
    return update_count > 0;
}