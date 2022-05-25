#ifndef safe_queue_hpp
#define safe_queue_hpp

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
    SafeQueue(int thread_num);
    SafeQueue(SafeQueue const& other);
    SafeQueue(queue<string> const& default_queue, int thread_num);
    void push(string name);
    int size();
    void set_me_working(int th, bool val);
    bool is_everybody_working();
    string just_pop();
    bool wait_pop(string& a, string& b);
};

#endif
