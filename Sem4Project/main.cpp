#include <iostream>
#include <thread>
#include <mutex>
#include <fstream>
#include <random>
#include <stdio.h>
#include <string>
#include <sstream>
#include <queue>

using namespace std;

class SafeQueue{
private:
    queue<string> q;
    mutable mutex m;
    mutable mutex vector_m;
    vector<bool> everybody_works;
    condition_variable c;
public:
    SafeQueue(int thread_num) {
        everybody_works.assign(thread_num,false);
    }
    
    SafeQueue(SafeQueue const& other) {
        lock_guard<mutex> g(other.m);
        lock_guard<mutex> gv(other.vector_m);
        q = other.q;
        everybody_works = other.everybody_works;
    }
    
    SafeQueue(queue<string> const& default_queue, int thread_num) {
        q = default_queue;
        everybody_works.assign(thread_num,false);
    }

    void push(string name) {
        lock_guard<mutex> g(m);
        q.push(name);
        c.notify_one();
    }
    
    int size() {
        lock_guard<mutex> g(m);
        return q.size();
    }
    
    void set_me_working(int th, bool val) {
        lock_guard<mutex> g(vector_m);
        everybody_works[th] = val;
        c.notify_one();
    }
    
    bool is_everybody_working() {
        lock_guard<mutex> g(vector_m);
        return accumulate(everybody_works.begin(), everybody_works.end(), false); //is ANYONE working
    }
    
    string just_pop() {
        lock_guard<mutex> lk(m);
        if(q.empty())
            throw "No elements";
        string a = q.front();
        q.pop();
        return a;
    }
    
    bool wait_pop(string& a, string& b) {
        unique_lock<mutex> lk(m);
        c.wait(lk, [this]{ return q.size()>1 || !is_everybody_working();});
        if(q.empty()) {
            throw "Error pop";
        }
        if(q.size()==1) {
            return false;
        }
        a = q.front();
        q.pop();
        b = q.front();
        q.pop();
        return true;
    }
};


int read_file(FILE *f, vector<int> &rbuf, int &size_of_divisions) {
    size_t sz = fread(rbuf.data(), sizeof(rbuf[0]), rbuf.size(), f);
    if (sz != size_of_divisions && sz!=0) {
        size_of_divisions = sz;
        rbuf.resize(size_of_divisions);
    }
    return sz;
}

void thread_work(SafeQueue& q, int my_num, FILE *f, int size_part) {
    vector<int> rbuf(size_part);
    int f_count = 0;
    auto myid = this_thread::get_id();
    stringstream ss;
    ss << myid;
    string thread_id = ss.str();
    while(read_file(f, rbuf, size_part ) != 0) {
        string f_name;
        f_name = "res" + to_string(f_count) + "_" + thread_id + ".bin";
        cout<< f_name <<endl;
        int n = f_name.length();
        char char_array[n + 1];
        strcpy(char_array, f_name.c_str());
        sort(rbuf.begin(), rbuf.end());
        std::ofstream new_file{f_name, std::ios::out | std::ios::binary};
        for (const auto& value : rbuf) {
            new_file.write(reinterpret_cast<const char*>(&value), sizeof(value));
        }
        new_file.close();
        f_count++;
        q.push(f_name);
    }
}

void process(vector<thread> &threads, FILE *f, SafeQueue &q) {
    try {
         int size_part = 262144*100; //50 Mb
         int num_threads = threads.size();
         for(int i = 0; i < num_threads;i++) {
              threads[i] = thread(thread_work, ref(q), i, ref(f), size_part);
         }
        for_each(threads.begin(), threads.end(), mem_fn(&thread::join));
    }
    catch(...) {
        cout << "Exception. Something wrong processing!" << endl;
        throw;
    }
}

void merge_work(SafeQueue& q, string &endname, int my_num) {
    auto myid = this_thread::get_id();
    stringstream ss;
    ss << myid;
    string thread_id = ss.str();
    int f_count = 0;
    string name_first, name_second;
    while(q.wait_pop(name_first, name_second)) {
        f_count++;
        q.set_me_working(my_num, true);

        FILE * file_first;
        int n = name_first.length();
        char char_array_one[n + 1];
        strcpy(char_array_one, name_first.c_str());
        file_first = fopen(char_array_one, "rb");
        
        FILE * file_second;
        n =name_second.length();
        char char_array_two[n + 1];
        strcpy(char_array_two, name_second.c_str());
        file_second = fopen(char_array_two, "rb");

        int a,b;
        int i = fread(&a, sizeof(int), 1, file_first);
        int j = fread(&b, sizeof(int), 1, file_second);
        if (i != 1 || j!=1) {
            fputs ("reading error",stderr);
        }
        string name;
        name = "mer_" + to_string(f_count)+ "_"+thread_id+ ".bin";
        n = name.length();
        char char_array[n + 1];
        strcpy(char_array, name.c_str());
        FILE * file_result;
        file_result = fopen(char_array, "wb");
        while (i>0 && j>0) {
            if(a<b) {
                int ret = fwrite(&a, sizeof(int), 1, file_result);
                if(ret!=1) {cout<< "writing error:("<<endl;}
                
                i = fread(&a, sizeof(int), 1, file_first);
                continue;
            }
            else {
                int ret = fwrite(&b, sizeof(int), 1, file_result);
                if(ret!=1) {cout<< "writing error:("<<endl;}
                
                j = fread(&b, sizeof(int), 1, file_second);
                continue;
            }
        }
        if(i == 0 && j!= 0) {
            while (j!=0) {
                int ret = fwrite(&b, sizeof(int), 1, file_result);
                if(ret!=1){cout<< "writing error:("<<endl;}
                j = fread(&b, sizeof(int), 1, file_second);
            }
        }
        if(j ==0 && i!= 0) {
            while (i!=0) {
                int ret = fwrite(&a, sizeof(int), 1, file_result);
                if(ret!=1){cout<< "writing error:("<<endl;}
                i = fread(&a, sizeof(int), 1, file_first);
            }
        }
    //    int status = remove(char_array_one);
    //    status = remove(char_array_two);
        fclose(file_result);
        q.push(name);
        endname = name;
        cout<< "first: " << name_first << " second: " << name_second <<  " merge: " <<  name <<endl;
                
        q.set_me_working(my_num, false);
    }
    q.set_me_working(my_num, false);
}

void merging(vector<thread> &threads, SafeQueue &q, string &endname) {
    cout<<"Merging"<<endl;
    int num_threads = threads.size();
    try {
         for(int i = 0; i < num_threads;i++) {
              threads[i] = thread(merge_work, ref(q),ref(endname), i);
         }
        for_each(threads.begin(), threads.end(), mem_fn(&thread::join));
    }
    catch(...) {
        cout << "Exception. Something wrong merging!" << endl;
        throw;
    }
}

int main(int argc, char**argv) {
    FILE * writefile;
    int r;
    writefile = fopen("test.bin", "wb");
    if (writefile == NULL) {
        perror("Error creating file!");
        return 0;
    }
    int int_in_Mb = 262144*1024;
    for (int i = 0; i < int_in_Mb; i++) {
        int p = rand()%100;
        r += fwrite(&p,sizeof(int),1,writefile);
    }
    fclose(writefile);
    
    //open for reading
    FILE * myfile;
    myfile = fopen("test.bin", "rb");
    queue<string> default_queue;
    int req_num_treads = 8;
    int max_threads = thread::hardware_concurrency();
    int num_threads = min(max_threads,req_num_treads);
    vector<thread> threads(num_threads);
    SafeQueue q(default_queue, num_threads);
    
    
    auto start = std::chrono::steady_clock::now();
    
    try {
        process(threads, myfile, q);
    }
    catch(...) {
        cout<<"Error processing file!"<<endl;
        return 0;
    }
    fclose(myfile);
    
    string end_name;
    try {
        merging(threads,q,end_name);
    }
    catch(...) {
        cout<<"Error mergering file!"<<endl;
        return 0;
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    cout<< "Done by "<< num_threads <<" thread(s) in "<<elapsed.count()<<" millisecs."<<endl;
    
    cout << "OK" << endl;
    cout << "Result file: " << end_name << endl;
    return 0;
}
