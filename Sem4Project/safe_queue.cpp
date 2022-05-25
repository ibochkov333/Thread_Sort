#include "safe_queue.hpp"

SafeQueue::SafeQueue(int thread_num) {
    everybody_works.assign(thread_num,false);
}
    
SafeQueue::SafeQueue(SafeQueue const& other) {
    lock_guard<mutex> g(other.m);
    lock_guard<mutex> gv(other.vector_m);
    q = other.q;
    everybody_works = other.everybody_works;
}
    
SafeQueue::SafeQueue(queue<string> const& default_queue, int thread_num) {
    q = default_queue;
    everybody_works.assign(thread_num,false);
}

void SafeQueue::push(string name) {
    lock_guard<mutex> g(m);
    q.push(name);
    c.notify_one();
}
    
int SafeQueue::size() {
    lock_guard<mutex> g(m);
    return q.size();
}
    
void SafeQueue::set_me_working(int th, bool val) {
    lock_guard<mutex> g(vector_m);
    everybody_works[th] = val;
    c.notify_one();
}
    
bool SafeQueue::is_everybody_working() {
    lock_guard<mutex> g(vector_m);
    return accumulate(everybody_works.begin(), everybody_works.end(), false); //is ANYONE working
}
    
string SafeQueue::just_pop() {
    lock_guard<mutex> lk(m);
    if(q.empty())
        throw "No elements";
    string a = q.front();
    q.pop();
    return a;
}
    
bool SafeQueue::wait_pop(string& a, string& b) {
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
