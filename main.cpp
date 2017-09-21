//Nicolas Stoian
//This program needs 4 command line arguments
//argv[1] "input1" for text file representing the data dependency pairs
//argv[2] "input2" for text file representing the data job times
//argv[3] "input3" for integer representing the number of processors
//argv[3] "output1" to write the schedule table

#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>

using namespace std;

class ListNode{
private:
    int jobId;
    int time;
    ListNode* next;

public:
    ListNode();
    ListNode(int jobId, int time);
    ~ListNode();
    int getJobId();
    int getTime();
    ListNode* getNext();
    void setNext(ListNode* next);
};

class TableNode{
private:
    int jobId;
    TableNode* next;

public:
    TableNode();
    TableNode(int jobId, TableNode* next);
    ~TableNode();
    int getJobId();
    TableNode* getNext();
    void setNext(TableNode* next);
};

class LinkedList{
private:
    ListNode* listHead;

public:
    LinkedList();
    ~LinkedList();
    void insertListNode(ListNode* nodeToInsert);
    ListNode* getListHead();
    bool isEmpty();
    ListNode* removeListNode();
};

class HashTable{
private:
    TableNode** pointerArray;
    int numNodes;

public:
    HashTable(int numNodes, ifstream& inFile);
    ~HashTable();
    void insertTableNode(int nodeToAdd, int index);
    TableNode* elementAt(int index);
    bool isDependentOn(int index, int node);
    int getNumNodes();
};

void loadParentCount(HashTable* hashTable, int* parentCount, int numNodes);
void buildScheduleTable(int** scheduleTable, HashTable* hashTable, LinkedList* open, int* processJob, int* processTime, int* parentCount,
                        int* jobTime, int* jobDone, int* jobMarked, int numNodes, int procNeed, int& procUsed, int& time, int totalJobTimes);
void findOrphenNodes(int* parentCount, int* jobMarked, int* jobTime, LinkedList* open, int numNodes);
bool isGraphEmpty(int* jobDone, int numNodes);
void printToConsole(int** scheduleTable, HashTable* hashTable, LinkedList* open, int* processJob, int* processTime, int* parentCount,
                    int* jobTime, int* jobDone, int* jobMarked, int numNodes, int procNeed, int& procUsed, int& time, int totalJobTimes);
bool isProcessJobDone(int* processJob, int procNeed);
void outputHashTable(HashTable* hashTable, ofstream& outFile);
void outputJobTime(int* jobTime, int numNodes, ofstream& outFile);
void outputScheduleTable(int** scheduleTable, int time, int procNeed, ofstream& outFile);

int main(int argc, char* argv[])
{
    ifstream inFile;
    inFile.open(argv[1]);
    int numNodes;
    inFile >> numNodes;
    HashTable* hashTable = new HashTable(numNodes, inFile);
    ifstream inFile2;
    inFile2.open(argv[2]);
    int numNodes2;
    inFile2 >> numNodes2;
    if(numNodes != numNodes2){
        cout << "The number of nodes in the input files do not match, please check the arguments and try again";
        return 0;
    }
    stringstream procsInput(argv[3]);
    int procNeed;
    procsInput >> procNeed;
    if(procNeed > numNodes){
        procNeed = numNodes;
    }
    int* jobTime = new int [numNodes];
    int totalJobTimes = 0;
    int n1;
    int n2;
    while(inFile2 >> n1){
        inFile2 >> n2;
        jobTime[n1 - 1] = n2;
        totalJobTimes += n2;
    }
    int** scheduleTable;
    scheduleTable = new int* [numNodes];
    for(int i = 0; i < numNodes; i++){
        scheduleTable[i] = new int [totalJobTimes];
    }
    for(int row = 0; row < numNodes; row++){
        for(int col = 0; col < totalJobTimes; col++){
            scheduleTable[row][col] = -1;
        }
    }
    LinkedList* open = new LinkedList();
    int* processJob = new int [numNodes];
    int* processTime = new int [numNodes];
    for(int i = 0; i < procNeed; i++){
        processJob[i] = 0;
        processTime[i] = 0;
    }
    int* parentCount = new int [numNodes];
    int* jobDone = new int [numNodes];
    int* jobMarked = new int [numNodes];
    for(int i = 0; i < numNodes; i++){
        parentCount[i] = 0;
        jobDone[i] = 0;
        jobMarked[i] = 0;
    }
    int procUsed = 0;
    int time = 0;
    loadParentCount(hashTable, parentCount, numNodes);
    buildScheduleTable(scheduleTable, hashTable, open, processJob, processTime, parentCount, jobTime, jobDone, jobMarked, numNodes, procNeed, procUsed, time, totalJobTimes);
    ofstream outFile;
    outFile.open(argv[4]);
    outFile << "Input 1 data dependency pairs" << endl;
    outputHashTable(hashTable, outFile);
    outFile << endl;
    outFile << "Input 2 data job times" << endl;
    outputJobTime(jobTime, numNodes, outFile);
    outFile << endl;
    outFile << "Number of processors = " << procNeed << endl;
    outFile << endl;
    outputScheduleTable(scheduleTable, time, procNeed, outFile);
    delete scheduleTable;
    delete jobTime;
    delete open;
    delete hashTable;
    delete processJob;
    delete processTime;
    delete parentCount;
    delete jobDone;
    delete jobMarked;
    inFile.close();
    inFile2.close();
    outFile.close();
}

ListNode::ListNode(): jobId(-1), time(-1), next(NULL){
}

ListNode::ListNode(int jobId, int time): jobId(jobId), time(time), next(NULL){
}

ListNode::~ListNode(){
    next = NULL;
    delete next;
}

int ListNode::getJobId(){
    return jobId;
}

int ListNode::getTime(){
    return time;
}

ListNode* ListNode::getNext(){
    return next;
}

void ListNode::setNext(ListNode* next){
    this->next = next;
}

TableNode::TableNode(): jobId(-1), next(NULL){
}

TableNode::TableNode(int jobId, TableNode* next): jobId(jobId), next(next){
}

TableNode::~TableNode(){
    next = NULL;
    delete next;
}

int TableNode::getJobId(){
    return jobId;
}

TableNode* TableNode::getNext(){
    return next;
}

void TableNode::setNext(TableNode* next){
    this->next = next;
}

LinkedList::LinkedList(){
    listHead = new ListNode();
}

LinkedList::~LinkedList(){
    ListNode* walker = listHead;
    while (walker->getNext() != NULL){
        ListNode* prev = walker;
        walker = walker->getNext();
        delete prev;
    }
    delete walker;
    delete listHead;
}

ListNode* LinkedList::getListHead(){
    return listHead;
}

void LinkedList::insertListNode(ListNode* nodeToInsert){
    ListNode* walker = listHead;
    while(walker->getNext() != NULL){
        walker = walker->getNext();
    }
    walker->setNext(nodeToInsert);
}

bool LinkedList::isEmpty(){
    if(listHead->getNext() == NULL){
        return true;
    }
    else{
        return false;
    }
}

ListNode* LinkedList::removeListNode(){
    ListNode* toReturn = listHead->getNext();
    listHead->setNext(toReturn->getNext());
    toReturn->setNext(NULL);
    return toReturn;
}

HashTable::HashTable(int numNodes, ifstream& inFile){
    this->numNodes = numNodes;
    pointerArray = new TableNode* [numNodes];
    for(int i = 0; i < numNodes; i++){
        pointerArray[i] = new TableNode();
    }
    int n1;
    int n2;
    while(inFile >> n1){
        inFile >> n2;
        insertTableNode(n1, n2);
    }
}

HashTable::~HashTable(){
    TableNode* walker;
    for(int i = 0; i < numNodes; i++){
        walker = pointerArray[i];
        while(walker->getNext() != NULL){
            TableNode* prev = walker;
            walker = walker->getNext();
            delete prev;
        }
    }
    delete walker;
    delete pointerArray;
}

void HashTable::insertTableNode(int nodeToAdd, int index){
    TableNode* walker = pointerArray[index - 1];
    while(walker->getNext() != NULL){
        walker = walker->getNext();
    }
    walker->setNext(new TableNode(nodeToAdd, NULL));
}

TableNode* HashTable::elementAt(int index){
    return pointerArray[index];
}

bool HashTable::isDependentOn(int index, int node){
    TableNode* walker = pointerArray[index];
    while(walker->getNext() != NULL){
        if(walker->getNext()->getJobId() == node){
            return true;
        }
        walker = walker->getNext();
    }
    return false;
}

int HashTable::getNumNodes(){
    return numNodes;
}

void loadParentCount(HashTable* hashTable, int* parentCount, int numNodes){
    TableNode* walker;
    for(int i = 0; i < numNodes; i++){
        walker = hashTable->elementAt(i);
        int numParents = 0;
        while(walker->getNext() != NULL){
            numParents++;
            walker = walker->getNext();
        }
        parentCount[i] = numParents;
    }
}

void buildScheduleTable(int** scheduleTable, HashTable* hashTable, LinkedList* open, int* processJob, int* processTime, int* parentCount,
                        int* jobTime, int* jobDone, int* jobMarked, int numNodes, int procNeed, int& procUsed, int& time, int totalJobTimes){
    while(!isGraphEmpty(jobDone, numNodes)){    //Step 11
        findOrphenNodes(parentCount, jobMarked, jobTime, open, numNodes);   //Step 1
        while(!open->isEmpty() && !(procUsed >= procNeed)){     //Step 3
            int availProc = -1; // Step 2 Start
            for(int i = 0; i <= procNeed; i++){
                if(processJob[i] <= 0){
                    availProc = i;
                    break;
                }
            }
            if(availProc >= 0){
                ListNode* newJob = open->removeListNode();
                processJob[availProc] = newJob->getJobId();
                processTime[availProc] = newJob->getTime();
                cout << newJob->getJobId() << " " << newJob->getTime() << endl;
                for(int i = time; i < time + newJob->getTime(); i++){
                    scheduleTable[availProc][i] = newJob->getJobId();
                }
                procUsed++;
            }
        }   // Step 2 End
        if(open->isEmpty() && !isGraphEmpty(jobDone, numNodes) && isProcessJobDone(processJob, procNeed)){ // Step 4
            cout << "Error, cycle detected in the graph. Program exiting." << endl << endl;
            exit(1);
        }
        printToConsole(scheduleTable, hashTable, open, processJob, processTime, parentCount,
                       jobTime, jobDone, jobMarked, numNodes, procNeed, procUsed, time, totalJobTimes);  // Step 5
        time++; // Step 6
        for(int i = 0; i < procNeed; i++){  // Step 7
            processTime[i]--;
        }
        for(int i = 0; i < procNeed; i++){ // Step 9
            if(processTime[i] == 0){    // Step 8 Start
                int job = processJob[i] - 1;
                processJob[i] = 0;
                jobDone[job] = 1;
                for(int i = 0; i < numNodes; i++){
                    if(hashTable->isDependentOn(i, job + 1)){
                        parentCount[i]--;
                    }
                }
                procUsed--;
            }
        } // Step 8 End
        printToConsole(scheduleTable, hashTable, open, processJob, processTime, parentCount,
                       jobTime, jobDone, jobMarked, numNodes, procNeed, procUsed, time, totalJobTimes); // Step 10
    }
}

bool isGraphEmpty(int* jobDone, int numNodes){
    for(int i = 0; i < numNodes; i++){
        if(jobDone[i] == 0){
            return false;
        }
    }
    return true;
}

void findOrphenNodes(int* parentCount, int* jobMarked, int* jobTime, LinkedList* open, int numNodes){
    for(int i = 0; i < numNodes; i++){
        if(parentCount[i] == 0 && jobMarked[i] == 0){
            jobMarked[i] = 1;
            ListNode* nodeToInsert = new ListNode(i + 1, jobTime[i]);
            open->insertListNode(nodeToInsert);
        }
    }
}

bool isProcessJobDone(int* processJob, int procNeed){
    for(int i = 0; i < procNeed; i++){
        if(processJob[i] > 0){
            return false;
        }
    }
    return true;
}

void printToConsole(int** scheduleTable, HashTable* hashTable, LinkedList* open, int* processJob, int* processTime, int* parentCount,
                    int* jobTime, int* jobDone, int* jobMarked, int numNodes, int procNeed, int& procUsed, int& time, int totalJobTimes){
    cout << "scheduleTable" << endl;
    cout << right << setw(5) << "index";
    for(int col = 0; col < time; col++){
        stringstream t;
        t << "t" << col + 1;
        cout  << setw(4) << t.str();
    }
    cout << endl;
    for(int row = 0; row < procNeed; row++){
        stringstream p;
        p << "p" << row + 1;
        cout << setw(5) << p.str();
        for(int col = 0; col < time; col++){
            if(scheduleTable[row][col] == -1){
                cout << setw(4) << "-";
            }
            else{
                cout << setw(4) << scheduleTable[row][col];
            }
        }
        cout << endl;
    }
    cout << endl;
    cout << "open list" << endl;
    cout << "listHead -->";
    ListNode* walker = open->getListHead();
    while(walker->getNext() != NULL){
        cout << "(" << walker->getNext()->getJobId() << "," << walker->getNext()->getTime() << ") -->";
        walker = walker->getNext();
    }
    cout << "NULL" << endl;
    cout << endl;

    cout << "time == " << time << endl;
    cout << "procUsed == " << procUsed << endl;
    cout << endl;

    cout << "processJob" << endl;
    cout << right << setw(5) << "index";
    for(int i = 0; i < procNeed; i++){
        cout  << setw(3) << i;
    }
    cout << endl;
    cout << setw(5) << " ";
    for(int i = 0; i < procNeed; i++){
        cout  << setw(3) << processJob[i];
    }
    cout << endl << endl;
    cout << "processTime" << endl;
    cout << right << setw(5) << "index";
    for(int i = 0; i < procNeed; i++){
        cout  << setw(3) << i;
    }
    cout << endl;
    cout << setw(5) << " ";
    for(int i = 0; i < procNeed; i++){
        cout  << setw(3) << processTime[i];
    }
    cout << endl << endl;
    cout << "parentCount" << endl;
    cout << right << setw(5) << "index";
    for(int i = 0; i < numNodes; i++){
        cout  << setw(3) << i + 1;
    }
    cout << endl;
    cout << setw(5) << " ";
    for(int i = 0; i < numNodes; i++){
        cout  << setw(3) << parentCount[i];
    }
    cout << endl << endl;
    cout << "jobTime" << endl;
    cout << right << setw(5) << "index";
    for(int i = 0; i < numNodes; i++){
        cout  << setw(3) << i + 1;
    }
    cout << endl;
    cout << setw(5) << " ";
    for(int i = 0; i < numNodes; i++){
        cout  << setw(3) << jobTime[i];
    }
    cout << endl << endl;
    cout << "jobDone" << endl;
    cout << right << setw(5) << "index";
    for(int i = 0; i < numNodes; i++){
        cout  << setw(3) << i + 1;
    }
    cout << endl;
    cout << setw(5) << " ";
    for(int i = 0; i < numNodes; i++){
        cout  << setw(3) << jobDone[i];
    }
    cout << endl << endl;
    cout << "jobMarked" << endl;
    cout << right << setw(5) << "index";
    for(int i = 0; i < numNodes; i++){
        cout  << setw(3) << i + 1;
    }
    cout << endl;
    cout << setw(5) << " ";
    for(int i = 0; i < numNodes; i++){
        cout  << setw(3) << jobMarked[i];
    }
    cout << endl << endl;
}

void outputHashTable(HashTable* hashTable, ofstream& outFile){
    outFile << hashTable->getNumNodes() << endl;
    TableNode* walker;
    for(int i = 0; i < hashTable->getNumNodes(); i++){
        walker = hashTable->elementAt(i);
        while(walker->getNext() != NULL){
            outFile << walker->getNext()->getJobId() << " " << i + 1 << endl;
            walker = walker->getNext();
        }
    }
}

void outputJobTime(int* jobTime, int numNodes, ofstream& outFile){
    outFile << numNodes << endl;
    for(int i = 0; i < numNodes; i++){
        outFile << i + 1 << " " << jobTime[i] << endl;
    }
}

void outputScheduleTable(int** scheduleTable, int time, int procNeed, ofstream& outFile){
    outFile << "scheduleTable" << endl;
    outFile << right << setw(5) << "index";
    for(int col = 0; col < time; col++){
        stringstream t;
        t << "t" << col + 1;
        outFile  << setw(4) << t.str();
    }
    outFile << endl;
    for(int row = 0; row < procNeed; row++){
        stringstream p;
        p << "p" << row + 1;
        outFile << setw(5) << p.str();
        for(int col = 0; col < time; col++){
            if(scheduleTable[row][col] == -1){
                outFile << setw(4) << "-";
            }
            else{
                outFile << setw(4) << scheduleTable[row][col];
            }
        }
        outFile << endl;
    }
    outFile << endl;
}
