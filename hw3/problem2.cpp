#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <math.h>
#include <cmath>
#include <fstream>
#include <string>
#include <bitset>
#include <iomanip>
using namespace std;

/*
Assume that we have a relation Employee(id, name, bio, manager-id). The values of id and
manager-id are character strings with fixed size of 8 bytes. The values of name and bio are
character strings and take at most 200 and 500 bytes. Note that as opposed to the values of id
and manager-id, the sizes of the values of name (bio) are not fixed and are between 1 to 200 (500)
bytes. The size of each block is 4096 bytes (4KB). The size of each record is less than the size of a
block. 

Write a C++ program that reads an input Employee relation and builds a linear hash
index for the relation using attribute id. You may use a hash function of your choice. Your
program must increment the value of n if the average number of records per each block exceeds
80% of the block capacity. The input relation is stored in a CSV file, i.e., each tuple is in a
separate line and fields of each record are separated by commas. The program must also support
looking up a tuple using its id.
    
    • The program must accept switch C for index creation mode and L for lookup in its
    command line. The switch L is succeeded by the value of input id.

    • Your program must assume that the input CSV file is in the current working directory, i.e.,
    the one from which your program is running, and its name is Employee.csv

    • The program must store the indexed relation in a file with the name EmployeeIndex on the
    current working directory.

    • Your program must run on Linux. Each student has an account on
    hadoop-master.engr.oregonstate.edu server, which is a Linux machine. You may use this 
    machine to test your program if you do not have access to any other Linux machine. You
    can use the following bash command to connect to voltdb1 :
        > ssh your_onid_username@hadoop-master.engr.oregonstate.edu
        Then it asks for your ONID password and probably one another question. You can only
        access this server on campus.

    • You can use following commands to compile and run C++ code:
        > g++ main.cpp -o main.out
        > main.out */

// globals
int i; // number of trailing bits
int n; // number of buckets
int r; // total occupied slots
int y = 2; // slots per bucket

//MAP STRUCTURE
//
//0|--
//1|--
//2|--
//
// To get index from employeeId:
// h(id) produces a condensed int - see: strhash()
// h(id) is then converted to binary - see: create(), lines 285-290
//      binary is then trimmed according to i value. last i places of binary fetched
// the trimmed binary hash is converted to decimal, used as our hash index. see: create(), 290+
//      converted using dec_to_binary and binary_to_dec.

int dec_to_binary(int value){
    return stoi(bitset<32>(value).to_string());
}

int binary_to_dec(int binary){
    long int bin, dec=0, i=1, rem;
	bin = binary;
	while(bin != 0)
	{
		rem = bin % 10;
		dec = dec + rem * i;
		i = i * 2;
		bin = bin/10;
	}
    return dec;
}

struct Employee{
    string id = ""; // = 8 b 
    string managerid = ""; // = 8 b
    string name = ""; //max 200 b
    string bio = ""; //max 500 b
    int size;
    void get_employee_size(){
        size = bio.size() + managerid.size() + name.size() + bio.size();
    };
    //max size of employee = 716 b
    //each record less than a block
};


class Block{
public:
    int index;
    Employee slots[2];
    Block *overflow;
    Block *next;
    Block();
};
Block::Block(){
    this->slots[0] = {"0", "0", "0", "0"};
    this->slots[1] = {"0", "0", "0", "0"};
}


Block* init_map(){
    Block* start = new Block();
    Block* second = new Block();
    Block* third = new Block();
    Block* fourth = new Block();

    start->index = 0;
    start->overflow = NULL;
    second->index = 1;
    second->overflow = NULL;
    third->index = 2;
    third->overflow = NULL;
    fourth->index = 3;
    fourth->overflow = NULL;
    
    third->next = fourth;
    second->next = third;
    start->next = second;
    return start;
};

Block* last(Block* ptr){
    if (!ptr->next){
        return ptr;
    }
    else{
        ptr = last(ptr->next);
        return ptr;
    }
};

int strhash(string str){
    int i;
	int r = 0;
	for (i = 0; str[i] != '\0'; i++)
		r += str[i];
	return r;
}

int map_add(Employee E, Block* ptr){
    if (ptr->slots[0].id == "0"){
        ptr->slots[0] = E;
        r++;
        return 0;
    }
    else if (ptr->slots[1].id == "0"){
        ptr->slots[1] = E;
        r++;
        return 0;
    }
    return 1;
}


void add_at_overflow(Employee E, Block* ptr){
    // TOOD - here
    if (!ptr->overflow){ // if overflow slot uninitialized
        ptr->overflow = new Block();
    }
    int mapret = map_add(E, ptr->overflow);
    if (mapret == 1) { //exists but full, traverse to end and add
        add_at_overflow(E, ptr->overflow);
    }
};





void push_map(Block* map, Block* pushed){
    last(map)->next = pushed;
    n++;
};

Employee fetch(string emp_id, Block* ptr){
    if(ptr->slots[0].id == emp_id){
        return ptr->slots[0];
    }
    else if(ptr->slots[1].id == emp_id){
        return ptr->slots[1];
    }
    else if(ptr->overflow){
        return fetch(emp_id, ptr->overflow);
    }
    return {"0","0","0","0"};
}


Block* find_at_index(Block* ptr, int index){
    if (ptr->index == index) {
        return ptr;
    }
    else{
        ptr = find_at_index(ptr->next, index);
        return ptr;
    }
    return NULL;
};

void add_at_index(Employee E, Block* map, int index){
    Block* bucket = find_at_index(map, index);
    int ret = map_add(E, bucket);
    if (ret == 1){
        add_at_overflow(E, bucket);
    }
};

Employee find_employee(string emp_id, Block* map){
    int hid = strhash(emp_id); // hashed id 
    bitset<8> idbits(hid); // hashed id in binary
    string stridbits = idbits.to_string(); // binary hash in string
    int hk = stoi(stridbits.substr(stridbits.length()-i,i));
    int hashkey = binary_to_dec(hk);
    Block* location = find_at_index(map, hashkey);
    if (location){
        Employee target = fetch(emp_id, location);
        return target;
    }
    else{
        return {"0", "0", "0", "0"};
    }
}






void write_index(string index, string emp_id, string filename){
    ofstream file;
    file.open(filename, ios_base::app);
    file << index << ":" << emp_id << endl;
    file.close();
}

void write_csv(Employee E, string filename){
    ofstream file;
    file.open(filename);
    string stremp = E.id + "," + E.managerid + "," + E.name + "," + E.bio;
    file << stremp;
    file.close();
}

bool index_exists(int index, Block* ptr){
    if (ptr->index == index){
        return true;
    }
    else if (ptr->next){
        index_exists(index, ptr);
    }
    else {
        return false;
    }
}



Employee read_csv(string filename){
    ifstream file;
    string value;
    Employee E;
    string values[4];
    string emp_id;
    int i = 0;

    file.open(filename);
    char c[1000];
    while(file.good()){
        getline(file,value,'\n');
        cout << value << endl;
    }
    cout << "Enter an ID to add the tuple to the map: " << endl;
    cin >> emp_id;
    file.close();
    file.open(filename);
    bool write = false;
    while (file.good()){
         while(getline(file, value, '\n')){ 
            stringstream line(value);
            write = false;
            while(getline(line, value, ',')){
                if(value == emp_id){
                    write = true;
                }
                if (write){
                    values[i] = value;
                    i++;
                }
            }
        }
    }

    E.id = values[0];
    E.managerid = values[1];
    E.name = values[2];
    E.bio = values[3];
    E.get_employee_size();

    return E;
};


void lookup(Block* map){
    string emp_id;
    cout << "Enter an Employee ID to look up: ";
    cin >> emp_id;
    cout << "Looking up..." << endl;
    Employee temp = find_employee(emp_id, map);
    if(temp.id == emp_id){
        cout << "Employee found: " << temp.id << ", " << temp.managerid << ", " << temp.name << ", " << temp.bio << endl;
    }
    else{
        cout << "Employee id " << emp_id << " not found." << endl;
    }
}

void create(Block* map){
    Employee E = read_csv("Employee.csv");
    int k = strhash(E.id); // hashed id 
    bitset<36> bits(k); // hashed id in binary
    string strbits = bits.to_string(); // binary hash in string
    int m = stoi(strbits.substr(strbits.length()-i,i)); // last i bits of binary hash.
    int hashkey = binary_to_dec(m); // convert to decimal form

    // i, n, k, m

    add_at_index(E, map, hashkey);
    setprecision(2);
    double avg_capacity = ((double)r/((double)n*(double)y));
    if (avg_capacity > 0.80){                   // create if over avg capacity
        Block* new_bucket = new Block();        // init empty bucket          
        int last_index = last(map)->index;      // get the last index in binary
        int new_last_index;
        if (last_index == n-1){
            n++;                                // increase number of buckets
            new_last_index = last_index++;      // create new last index
            new_bucket->index = new_last_index;
            if (to_string(dec_to_binary(new_last_index)).length() > i){     //if binary length greater than current (i.e. 100 (4) > 11 (3)), increment it
                i++;
            }
            push_map(map, new_bucket);
        }
    }
    
    // check bucket at m
        // if space, add. increment r
        // if no space, overflow
    // check avg capacity per bucket
        // if > 80%, add buckets (increase n)
        // r /(n*y) r = total occupied slots, y = slots per bucket
    write_index(to_string(m), E.id, "EmployeeIndex");

}

int main(int argc, char *argv[]){

    i = 2; // init num bits to use
    n = 2; // init num buckets to use
    r = 0; // init occupied slots to 0
    Block* map_head = init_map();

    cout << "(C) : Create index" << endl << "(L) : Lookup index" << endl << "Press any other key to exit" << endl;
    for(;;){
        cout << "~: ";
        string cmd;
        cin >> cmd;
        if (cmd == "C" or cmd == "c"){
            create(map_head);
        }
        else if (cmd == "L" or cmd == "l"){
            lookup(map_head);
        }
        else{
            break;
        }
    } 

    // TODO init hash table
    // i = 2, should look like this
    // 00 |-- 
    // 01 |--

    // TODO add E to hash table at block m
        // TODO check avg capacity per block, add buckets (increase n) if > 80%
    
    // TODO lookup E from hash table given strhash(E.id)

    // TODO store indexed relation in EmployeeIndex file

    // write_csv(E, "output.csv"); 

    return 0;
};

