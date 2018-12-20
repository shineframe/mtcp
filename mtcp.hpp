#pragma once

#ifdef _WIN32
//define something for Windows (32-bit and 64-bit, this part is common)
#define MTCP_OS_WINDOWS
#define MTCP_OS "WINDOWS"
#ifdef _WIN64
//define something for Windows (64-bit only)
#else
//define something for Windows (32-bit only)
#endif
#elif __APPLE__
#include "TargetConditionals.h"
#if TARGET_IPHONE_SIMULATOR
// iOS Simulator
#define MTCP_OS_IOS_SIMULATOR
#define MTCP_OS "IOS_SIMULATOR"
#elif TARGET_OS_IPHONE
// iOS device
#define MTCP_OS_IOS_PHONE
#define MTCP_OS "IOS_PHONE"
#elif TARGET_OS_MAC
// Other kinds of Mac OS
#define MTCP_OS_MAC
#define MTCP_OS "MAC"
#else
#   error "Unknown Apple platform"
#endif
#elif __ANDROID__
// android
#define MTCP_OS_ANDROID
#define MTCP_OS "ANDROID"
#elif __linux__
// linux
#define MTCP_OS_LINUX
#define MTCP_OS "LINUX"
#elif __unix__ // all unices not caught above
// Unix
#define MTCP_OS_UNIX
#define MTCP_OS "UNIX"
#elif defined(_POSIX_VERSION)
// POSIX
#define MTCP_OS_POSIX "POSIX"
#define MTCP_OS "POSIX"
#else
#   error "Unknown compiler"
#endif


#if (defined MTCP_OS_WINDOWS)

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "Mswsock.lib")
typedef SOCKET mtcp_socket_t;
#define mtcp_invalid_socket INVALID_SOCKET

#else
#include <cstring>
#include<netdb.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
typedef int mtcp_socket_t;
#define mtcp_invalid_socket -1

void mtcp_handle_pipe(int sig) {}

#endif

#include <iostream>
#include <chrono>
#include <ctime>
#include <string>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <list>
#include <memory>
#include <map>
#include <set>
#include <vector>
#include <forward_list>
#include <functional>



#define mtcp_invalid_timer_id 0

namespace mtcp
{
//message access
    class session_t;

    class callback_t{
    public:
        virtual bool on_open(session_t* session) { return true; }
        virtual bool on_recv(session_t* session, unsigned int sequence, const char *data, std::size_t len) { return true; }
        virtual bool on_send(session_t* session, unsigned int sequence, int result, std::size_t cost_time) { return true; }
        virtual bool on_timeout(session_t* session) { return false; }
        virtual void on_close(session_t* session) { return; }
    };

    typedef std::function<unsigned int(session_t* session, const char *data, std::size_t len)> send_t;
    typedef std::function<bool(session_t* session)> active_callback_t;
    typedef std::function<bool(session_t* session, unsigned int sequence, const char *data, std::size_t len)> recv_callback_t;
    typedef std::function<bool(session_t* session, unsigned int sequence, int result, std::size_t cost_time)> send_callback_t;
    typedef std::function<bool(session_t* session)> timeout_callback_t;
    typedef std::function<void(session_t* session)> close_callback_t;

    typedef std::deque<session_t*> delay_close_queue_t;

    static inline bool before(unsigned int seq1, unsigned int seq2){
        return (int)(seq1 - seq2) < 0;
    }

    static inline bool after(unsigned int seq1, unsigned int seq2){
        return before(seq2, seq1);
    }

    static inline bool contain(unsigned int left, unsigned int right, unsigned int seq){
        return (left == seq || before(left, seq)) && ((right == seq) || after(right, seq));
    }

    //mtcp inner protocol define
    namespace protocol{
        enum {
            e_mss = 65400,
            e_wnd = 256,
            e_rtoc = 3,
        };

        enum {
            e_sync = 0,
            e_fin = 1,
            e_alive = 2,
            e_close = 3,
        };

        class slot_t{
        public:
            slot_t(){
                clear();
            }

            void set(unsigned int pos, bool val){
                pos &= 0x1F;
                unsigned int tmp = 1 << (7 - pos % 7);
                if (val)
                    slot[pos >> 3] |= tmp;
                else
                    slot[pos >> 3] &= ~tmp;
            }

            bool get(unsigned int pos){
                pos &= 0x1F;
                return (slot[pos >> 3] & (1 << (7 - pos % 7))) != 0;
            }

            void clear(){
                memset(slot, 0, sizeof(slot));
            }

        private:
            unsigned char slot[4];
        };

#pragma pack(push, 1)
        struct header_t{
            header_t(){
                memset(&flags, 0, sizeof(flags));
            }
            enum {
                e_sync = 1 << 7,
                e_push = 1 << 6,
                e_begin = 1 << 5,
                e_end = 1 << 4,
                e_ack = 1 << 3,
                e_sack = 1 << 2,
                e_heart = 1 << 1,
                e_fin = 1,
            };

            void hton(){
                seq = htonl(seq);
                group = htonl(group);
                token = htons(token);
            }

            void ntoh(){
                seq = ntohl(seq);
                group = ntohl(group);
                token = ntohs(token);
            }

            struct {
                unsigned char sync : 1;
                unsigned char push : 1;
                unsigned char begin : 1;
                unsigned char end : 1;
                unsigned char ack : 1;
                unsigned char s_ack : 1;
                unsigned char heartbeat : 1;
                unsigned char fin : 1;
            } flags;
            unsigned short token = 0;
            unsigned int seq = 0;
            unsigned int group = 0;
        };
#pragma pack(pop)

        class message_t {
            friend class session_t;
        public:
            message_t(unsigned int token){
                data_.resize(sizeof(header_t));
                head.token = token;
            }

            bool operator <(const message_t &other){
                return before(head.seq, other.head.seq);
            }

            bool operator ==(const message_t &other){
                return head.seq == other.head.seq;
            }

            static bool compare(const protocol::message_t * a, const protocol::message_t * b){
                return before(a->head.seq, b->head.seq);
            }

            static bool equal(const protocol::message_t * a, const protocol::message_t * b){
                return a->head.seq == b->head.seq;
            }

            char *data(){
                return (char*)data_.data() + sizeof(header_t);
            }

            size_t size() {
                return data_.size() - sizeof(header_t);
            }

            void assign(const char* data, std::size_t len){
                data_.resize(len + sizeof(header_t));
                if (len > 0)
                    memcpy((char*)data_.data() + sizeof(header_t), data, len);
            }

            void append(const char* data, std::size_t len){
                data_.append(data, len);
            }

            void insert(const char* data, std::size_t len){
                data_.insert(sizeof(header_t), data, len);
            }

            void clear(){
                data_.resize(sizeof(header_t));
            }

            const std::string &encode(){
//                 head.crc = crc((const unsigned char *)data(), size());
                header_t *h = (header_t*)data_.data();
                memcpy(h, &head, sizeof(header_t));
                h->hton();
                return data_;
            }

            const std::string &raw_data(){
                return data_;
            }

            static unsigned short crc(const unsigned char *data, std::size_t len){
                unsigned short ret = 0;
                for (std::size_t i = 1; i < len; i++)
                    ret += *(++data);

                return ret;
            }

            bool decode(const char* data, std::size_t len){
                if (len < sizeof(header_t) || len > e_mss + sizeof(header_t))
                    return false;

                header_t *h = (header_t*)data;
                h->ntoh();
//                 if (h->crc != crc((const unsigned char*)data + sizeof(header_t), len - sizeof(header_t)))
//                     return false;

                memcpy(&head, h, sizeof(header_t));

                data_.assign(data, len);
                return true;
            }

        public:
            header_t head;
            std::string data_;
            unsigned int send_count = 0;
            std::size_t timestamp = 0;
        };
    }

    struct group_compare{
        bool operator()(const unsigned int &a, const unsigned int & b){
            return before(a, b);
        }
    };

    struct message_compare{
        bool operator()(const protocol::message_t * a, const protocol::message_t * b){
            return before(a->head.seq, b->head.seq);
        }
    };

        using namespace std;
        struct address_info_t{
            std::string ip;
            unsigned short port;
        };

        typedef std::function<bool()> timer_callback_t;

        static inline std::size_t get_timestamp()
        {
            auto now = std::chrono::high_resolution_clock::now();
            return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        }

        struct timer_t
        {
            std::size_t id;
            std::size_t delay;
            std::size_t timestamp;
            timer_callback_t callback;
        };

        class timer_manager : std::multimap <std::size_t, timer_t>{
        public:
            std::size_t do_timer() {
                if (empty())
                    return 0;
                std::size_t now = get_timestamp();

                while (size() > 0)
                {
                    auto iter = begin();
                    if (iter->first <= now)
                    {
                        timer_t item = iter->second;
                        erase(iter);

                        if (item.callback()) {
                            item.timestamp = now + item.delay;
                            emplace(item.timestamp, std::move(item));
                        }
                    }
                    else {
                        break;
                    }
                }

                return size() > 0 ? begin()->first - now : 0;
            }

            std::size_t set_timer(std::size_t delay, timer_callback_t cb) {
                timer_t item;
                item.id = ++_id;
                item.delay = delay;
                item.timestamp = get_timestamp() + delay;
                item.callback = std::move(cb);

                auto ret = item.id;
                emplace(item.timestamp, std::move(item));
                return ret;
            }

            bool cancel_timer(std::size_t id) {
                if (id == mtcp_invalid_timer_id)
                    return false;

                for (auto iter = begin(); iter != end(); ++iter) {
                    if (id == iter->second.id) {
                        erase(iter);
                        return true;
                    }
                }

                return false;
            }
        private:
            std::size_t _id = 0;
        };

        class socket{
            friend class proactor_engine;
        public:

            static bool parse_addr(const string &addr, address_info_t &ret) {
                std::size_t pos = addr.find(":");
                if (pos == string::npos)
                    return false;

                ret.ip.assign(addr.c_str(), pos);
                ret.port = atoi(addr.c_str() + pos + 1);

                return !ret.ip.empty();
            }

            static bool get_local_addr(mtcp_socket_t fd, address_info_t &ret) {
                struct sockaddr_in sa;
                socklen_t len = sizeof(sa);
                if (!getsockname(fd, (struct sockaddr *)&sa, &len))
                {
                    ret.ip = inet_ntoa(sa.sin_addr);
                    ret.port = ntohs(sa.sin_port);
                    return true;
                }
                return false;
            }

            static bool get_remote_addr(mtcp_socket_t fd, address_info_t &ret) {
                struct sockaddr_in sa;
                socklen_t len = sizeof(sa);
                if (!getpeername(fd, (struct sockaddr *)&sa, &len))
                {
                    ret.ip = inet_ntoa(sa.sin_addr);
                    ret.port = ntohs(sa.sin_port);
                    return true;
                }
                return false;
            }

            static int get_error() {
#ifdef MTCP_OS_WINDOWS
                return WSAGetLastError();
#else
                return errno;
#endif
            }

            static const char *get_error_str(int err){
#ifdef MTCP_OS_WINDOWS
#pragma warning(disable:4996)
#endif
                return strerror(err);
            }

            static void dump_error(){
                int err = get_error();
                printf("error:%d, %s\n", err, get_error_str(err));
            }

            static void set_nodelay(mtcp_socket_t fd, bool val = true)
            {
                int opt = val ? 1 : 0;
                ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&opt, sizeof(opt));
            }

            static void set_noblock(mtcp_socket_t fd, bool val = true)
            {
#if (defined MTCP_OS_WINDOWS)  
                unsigned long opt = val ? 1 : 0;
                ::ioctlsocket(fd, FIONBIO, &opt);
#else
                int flags = fcntl(fd, F_GETFL, 0);
                if (val)
                    flags |= O_NONBLOCK;
                else
                    flags &= ~O_NONBLOCK;

                fcntl(fd, F_SETFL, flags);
#endif 
            }

/*
            int tcp_connect(const char *addr, short port)
            {
                char ip[128];
                memset(ip, 0, sizeof(ip));
                strcpy(ip, addr);

                void* svraddr = nullptr;
                int error = -1, svraddr_len;
                bool ret = true;
                struct sockaddr_in svraddr_4;
                struct sockaddr_in6 svraddr_6;

                //��ȡ����Э��
                struct addrinfo *result;
                error = getaddrinfo(ip, NULL, NULL, &result);
                if (error != 0)
                    return -1;

                struct sockaddr *sa = result->ai_addr;
                socklen_t maxlen = 128;

                int fd = -1;

                switch (sa->sa_family)
                {
                case AF_INET://ipv4
                {
                                 if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                                     ret = false;
                                     break;
                                 }

                                 char *tmp = (char *)ip;
                                 if (inet_ntop(AF_INET, (void *)&(((struct sockaddr_in *) sa)->sin_addr), tmp,
                                     maxlen) == NULL) {
                                     ret = false;
                                     break;
                                 }

                                 svraddr_4.sin_family = AF_INET;
                                 svraddr_4.sin_addr.s_addr = inet_addr(ip);
                                 svraddr_4.sin_port = htons(port);
                                 svraddr_len = sizeof(svraddr_4);
                                 svraddr = &svraddr_4;
                                 break;
                }
                case AF_INET6://ipv6
                {
                                  if ((fd = socket(AF_INET6, SOCK_STREAM, 0)) < 0) {
                                      ret = false;
                                      break;
                                  }

                                  inet_ntop(AF_INET6, &(((struct sockaddr_in6 *) sa)->sin6_addr), ip, maxlen);

                                  memset(&svraddr_6, 0, sizeof(svraddr_6));
                                  svraddr_6.sin6_family = AF_INET6;
                                  svraddr_6.sin6_port = htons(port);

                                  if (inet_pton(AF_INET6, ip, &svraddr_6.sin6_addr) < 0) {
                                      ret = false;
                                      break;
                                  }

                                  svraddr_len = sizeof(svraddr_6);
                                  svraddr = &svraddr_6;
                                  break;
                }
                default: {
                             ret = false;
                }
                }

                freeaddrinfo(result);

                if (!ret)
                {
                    close_socket(fd);
                    return -1;
                }

                set_noblock(fd);

                int nret = ::connect(fd, (struct sockaddr*)svraddr, svraddr_len);

                if (nret != 0)
                {
                    int err = get_error();
#ifdef _WIN32
                    if (err != WSAEINPROGRESS && err != WSAEWOULDBLOCK)
                    {
                        close_socket(fd);
                        return -1;
                    }
#else
                    if (err != EINPROGRESS && err != EWOULDBLOCK)
                    {
                        close_socket(fd);
                        return -1;
                    }

#endif
                    else
                    {
                        struct timeval timeout = { 2, 0 };
                        fd_set wset, rset;
                        FD_ZERO(&wset);
                        FD_ZERO(&rset);
                        FD_SET(fd, &wset);
                        FD_SET(fd, &rset);

                        int res = select(fd + 1, &rset, &wset, NULL, &timeout);

                        if (res <= 0)
                        {
                            goto CONNECT_ERROR;
                        }
                        else if (1 == res)
                        {
                            if (FD_ISSET(fd, &wset))
                            {
                                set_noblock(fd, true);
                                return fd;
                            }
                            else
                            {
                                goto CONNECT_ERROR;
                            }
                        }
                    }
                }
                else
                {
                    return fd;
                }

            CONNECT_ERROR:
                close_socket(fd);
                return -1;
            }
*/

            static bool connect(mtcp_socket_t fd, const string &addr/*ip:port*/){
                address_info_t info;
                if (!parse_addr(addr, info))
                    return false;

                struct sockaddr_in address;
                address.sin_family = AF_INET;
                address.sin_port = htons(info.port);
                address.sin_addr.s_addr = inet_addr(info.ip.c_str());

                return connect(fd, (struct sockaddr *)&address);
            }

            static bool connect(mtcp_socket_t fd, const struct sockaddr *addr/*ip:port*/){
                auto rc = ::connect(fd, addr, sizeof(sockaddr_in));
                return true;
            }


            static bool bind(mtcp_socket_t fd, const string &addr/*ip:port*/){
                address_info_t info;
                if (!parse_addr(addr, info))
                    return false;

                int opt = 1;
                setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#if (defined MTCP_OS_WINDOWS)
#else
#if (defined SO_REUSEPORT)
                setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif
#endif

                struct sockaddr_in address;
                address.sin_family = AF_INET;
                address.sin_port = htons(info.port);
                address.sin_addr.s_addr = inet_addr(info.ip.c_str());

                return ::bind(fd, (struct sockaddr *)&address, sizeof(address)) == 0;
            }

            static bool listen(mtcp_socket_t fd, int backlog){
                return ::listen(fd, backlog) == 0;
            }

            static mtcp_socket_t accept(mtcp_socket_t fd) {
                struct sockaddr_in addr;
                memset(&addr, 0, sizeof(addr));
#ifdef MTCP_OS_WINDOWS
                int addr_len = sizeof(addr);
#else
                unsigned int addr_len = sizeof(addr);
#endif
                return ::accept(fd, (sockaddr*)&addr, &addr_len);
            }

            static void close(mtcp_socket_t fd)
            {
                if (fd == mtcp_invalid_socket)
                    return;
#if (defined MTCP_OS_WINDOWS)
                ::closesocket(fd);
#else
                ::close(fd);
#endif
            }

            static mtcp_socket_t create(int af, int type, int protocol = 0)
            {
                init();
                mtcp_socket_t s = mtcp_invalid_socket;
#if (defined MTCP_OS_WINDOWS)
                af = PF_INET;
                s = WSASocket(af, type, protocol, 0, 0, WSA_FLAG_OVERLAPPED);
#else
                s = ::socket(af, type, protocol);
#endif
                return s;
            }

#if (defined MTCP_OS_WINDOWS)
            static bool load_wsa_func(GUID &guid, void *&pfun) {
                static once_flag t;
                static mtcp_socket_t fd = mtcp_invalid_socket;
                std::call_once(t, []{
                    fd = create(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                });

                DWORD tmp = 0;
                return ::WSAIoctl(fd, SIO_GET_EXTENSION_FUNCTION_POINTER, &guid
                    , sizeof(guid), &pfun, sizeof(pfun), &tmp, NULL, NULL) != SOCKET_ERROR;
            }
#endif

        private:
            static void init(){
#if (defined MTCP_OS_WINDOWS)
                static once_flag t;
                std::call_once(t, []{
                    WSADATA wsa_data;
                    WSAStartup(0x0201, &wsa_data);
                });
#endif
            }
        };

        class peer_t;
        struct context
#if (defined MTCP_OS_WINDOWS)
            : OVERLAPPED
#endif
        {
            enum {
                e_recv = 0,
                e_send = 1,
                e_exit = 2
            };
            context()
            {
                buf.resize(protocol::e_mss + sizeof(protocol::header_t));
#if (defined MTCP_OS_WINDOWS)
                memset(this, 0, sizeof(OVERLAPPED));
                WSABuf.buf = (CHAR*)buf.data();
                WSABuf.len = (ULONG)buf.size();
#endif
                address.resize(sizeof(sockaddr_in));
            }

            int status = e_recv;
            string buf;

#if (defined MTCP_OS_WINDOWS)
            WSABUF WSABuf;
#endif
            string address;
            peer_t *peer;
        };

        class proactor_engine;
        class peer_t;
        class session_t;
        typedef std::function<bool(bool, peer_t *peer)> peer_callback_t;

        struct sequence_t{
            unsigned int count = 0;
            std::size_t timestamp = 0;
        };

        struct serial_t {
            unsigned int left = 0;
            unsigned int right = 0;
        };

        class session_t{
            friend class proactor_engine;
            friend class peer_t;
        public:
            enum {
                e_server = 0,
                e_server_peer = 1,
                e_client = 2,
            };

            session_t(unsigned int token, callback_t *cb)
                : local_token(token), callback(cb)
            {
            }
            ~session_t(){}

            const address_info_t & get_local_addr(){
                return local_addr_info;
            }

            const address_info_t & get_remote_addr(){
                return remote_addr_info;
            }

            bool on_timer(){
                std::size_t ts = get_timestamp();

                if (recv_timestamp && ts - recv_timestamp > timeout * protocol::e_rtoc)
                {
                    bool rc = callback->on_timeout(this);

                    if (!rc)
                    {
                        close();
                        return false;
                    }
                    else
                    {
                        recv_timestamp = ts;
                    }
                }

                if (!wait_ack_set.empty()) {
                    if (ts - (*wait_ack_set.begin())->timestamp > 40)
                    {
                        send(*wait_ack_set.begin());
                    }
                }

                flush();

                if (recv_timestamp && ts - send_timestamp > timeout)
                    send_heartbeat();

                return true;
            }

            unsigned int send(const char *data, std::size_t len) {
                if (len == 0)
                    return 0;

                unsigned int req_seq = ++req_sequence;
                unsigned int count = (unsigned int)(len / protocol::e_mss + (len % protocol::e_mss ? 1 : 0));
                sequence_t sequence_obj;
                sequence_obj.count = count;
                //std::cout << count << std::endl;
                sequence_obj.timestamp = get_timestamp();
                auto iter = sequence_map.insert(std::make_pair(req_seq, sequence_obj));
                std::size_t pos = 0;
                while (pos < len)
                {
                    protocol::message_t * message = new protocol::message_t(local_token);
                    std::size_t cost = (len > pos + protocol::e_mss) ? protocol::e_mss : (len - pos);
                    message->head.group = req_seq;
                    message->head.flags.begin = pos == 0 ? 1 : 0;
                    message->head.flags.end = (len <= pos + protocol::e_mss) ? 1 : 0;
                    message->head.flags.push = 1;
                    message->head.flags.ack = 0;
                    message->head.flags.s_ack = 0;
                    message->head.flags.heartbeat = 0;
                    message->head.flags.fin = 0;
                    message->head.seq = ++seq;
                    message->assign(data + pos, cost);
                    message->encode();

                    pos += cost;

                    if (wait_ack_set.size() == protocol::e_wnd) {
                        wait_send.emplace_back(std::move(message));
                    }
                    else {
                        send(message);
                        wait_ack_set.emplace(std::move(message));
                    }
                }

                return req_seq;
            }

            void close(){
                close(true);
            }

        private:
            void close(bool local) {
                reset();

                if (local)
                {
                    for (int i = 0; i < 3; i++)
                        send_fin();
                }

                if (type == e_client)
                    socket::close(fd);
                else
                    callback->on_close(this);
            }

            void reset(){
                seq = 0;
                req_sequence = 0;
                remote_commit_seq = 0;

                for (auto iter : recv_map) {
                    for (auto iter2 : iter.second)
                        delete iter2;
                }
                recv_map.clear();

                for (auto iter : wait_send)
                    delete iter;
                wait_send.clear();

                for (auto iter : wait_ack_set)
                    delete iter;
                wait_ack_set.clear();

            }

            void on_recv(protocol::message_t * message){
                if (message->head.flags.ack || message->head.flags.s_ack) {
                    on_ack(message);
                    delete message;
                    return;
                }

                //std::cout << message->head.seq << std::endl;    
                if (message->head.flags.push)
                {
//                     send_ack(message->head.seq, message->head.group);
//                     return; 
                    if (before(remote_commit_seq, message->head.seq))
                    {
                        auto iter = recv_map.find(message->head.group);
                        if (iter != recv_map.end())
                        {
                            group_message_t &group = iter->second;
                            auto sub_iter = group.insert(message);
                            if (!sub_iter.second)
                            {
                                send_ack(remote_commit_seq, message);
                                delete message;
                            }
                            else{
                                send_ack(consumer(), message);
                            }
                        }
                        else{
                            if (message->head.flags.begin && message->head.flags.end 
                                && message->head.seq == remote_commit_seq + 1)
                            {
                                remote_commit_seq = message->head.seq;
                                send_ack(remote_commit_seq, message);
                                callback->on_recv(this, message->head.group, message->data(), message->size());
                                delete message;
                            }
                            else {
                                group_message_t group = { message };
                                recv_map.emplace(message->head.group, std::move(group));

                                send_ack(consumer(), message);
                            }
                        }
                    }
                    else{
                        send_ack(remote_commit_seq, 0, true);
                        delete message;
                    }
                }

                flush();
            }

            void flush() {
                while (wait_send.size() > 0 && wait_ack_set.size() < protocol::e_wnd) {
                    send(wait_send[0]);
                    wait_ack_set.insert(wait_send[0]);
                    wait_send.pop_front();
                }
            }

            void check_sequence_count(unsigned int sequence){
                auto iter = sequence_map.find(sequence);
                if (iter != sequence_map.end()){
                    if (--iter->second.count == 0)
                    {
                        callback->on_send(this, iter->first, 0, get_timestamp() - iter->second.timestamp);
                        sequence_map.erase(iter);
                    }
                }
            }

            void on_ack(protocol::message_t * message){

                if (message->head.flags.s_ack) {
                    while (wait_ack_set.size() > 0 && 
                    ((*(*wait_ack_set.begin())) < (*message) || (*(*wait_ack_set.begin())) == (*message))){
                        check_sequence_count((*wait_ack_set.begin())->head.group);

                        delete *wait_ack_set.begin();
                        wait_ack_set.erase(wait_ack_set.begin());
                    }
                }
                else {
                    check_sequence_count(message->head.group);

                    auto iter = wait_ack_set.find(message);
                    if (iter != wait_ack_set.end())
                    {
                        delete *iter;
                        wait_ack_set.erase(iter);
                    }
                }
            }

            unsigned int consumer() {

                unsigned int rc = 0;
//                 return;
            BEGIN:
                auto iter = recv_map.begin(); 
                if (iter == recv_map.end())
                    return rc;

                group_message_t &group = iter->second;
                if (group.empty()) {
                    recv_map.erase(iter);
                    goto BEGIN;
                }

                if ((*group.begin())->head.seq != remote_commit_seq + 1)
                    return rc;

                auto iter2 = group.begin();

                protocol::header_t &begin = (*group.begin())->head;
                protocol::header_t &end = (*group.rbegin())->head;

                if (!(begin.flags.begin && end.flags.end && (begin.seq + group.size() - 1) == end.seq))
                    return rc;

                std::string data;
                unsigned int sequence = (*group.begin())->head.group;
                for (auto iter2 : group)
                {
                    data.append(iter2->data(), iter2->size());
                    remote_commit_seq = iter2->head.seq;
                    delete iter2;
                }

                recv_map.erase(iter);
                
                rc = remote_commit_seq;
                callback->on_recv(this, sequence, data.data(), data.size());

                goto BEGIN;
            }

            void send(protocol::message_t *message){
                auto rc = ::sendto(fd, message->raw_data().data(), (int)message->raw_data().size(), 0
                    , (struct sockaddr*)remote_address.data(), (int)remote_address.size());

                //std::cout << rc << ":"  << message->head.seq << std::endl;
                message->send_count++;
                message->timestamp = get_timestamp();
                send_timestamp = message->timestamp;
            }

            void send_sync(){
                protocol::message_t * message = new protocol::message_t(local_token);
                message->head.flags.sync = 1;
                message->encode();
                send(message);

                wait_ack_set.emplace(std::move(message));
            }

            void send_heartbeat(){
                protocol::message_t message(local_token);
                message.head.flags.heartbeat = 1;
                message.encode();
                send(&message);
            }

            void send_fin(){
                protocol::message_t message(local_token);
                message.head.flags.fin = 1;
                message.encode();
                send(&message);
            }

            void send_ack(unsigned int commit_seq, protocol::message_t *message){
                if (commit_seq < message->head.seq)
                    send_ack(message->head.seq, message->head.group, false);
                else
                    send_ack(commit_seq, 0, true);
            }

            void send_ack(unsigned int seq, unsigned int group, bool s_ack = false){
                protocol::message_t message(local_token);
                message.head.seq = seq;
                message.head.group = group;
                if (s_ack)
                    message.head.flags.s_ack = 1;
                else
                    message.head.flags.ack = 1;
                message.encode();
                send(&message);
            }
        private:
            int type = 0;
            unsigned int rtt = 50;
            unsigned int timeout = 5000;
            std::size_t recv_timestamp = 0;
            std::size_t send_timestamp = 0;
            int status = protocol::e_close;
            unsigned int local_token = 0;
            unsigned int remote_token = 0;
            address_info_t local_addr_info;
            address_info_t remote_addr_info;
            std::string remote_address;
            mtcp_socket_t fd = mtcp_invalid_socket;
            peer_t *peer = nullptr;
            unsigned int req_sequence = 0;
            std::size_t timer_id = mtcp_invalid_timer_id;
            unsigned int seq = 0;
            unsigned int remote_commit_seq = 0;

            std::unordered_map<unsigned int, sequence_t> sequence_map;
            typedef std::set<protocol::message_t *, message_compare> group_message_t;
            std::map<unsigned int, group_message_t, group_compare> recv_map;//key=group value=message set
            std::deque<protocol::message_t *> wait_send;
            std::set<protocol::message_t *, message_compare> wait_ack_set;
            callback_t *callback = nullptr;
            delay_close_queue_t *delay_close_queue = nullptr;
        };

        class session_impl_t;

        class peer_t {
            friend class proactor_engine;
        public:
            peer_t(){
                recv_ctx.status = context::e_recv;
                recv_ctx.peer = this;
            }
            virtual ~peer_t(){}

            virtual void async_recv()
            {
 #if (defined MTCP_OS_WINDOWS)
                DWORD bytes = 0;
                DWORD flags = 0;
                int addr_len = (int)recv_ctx.address.size();
                memset((void*)recv_ctx.address.data(), 0, recv_ctx.address.size());
                memset(&recv_ctx, 0, sizeof(OVERLAPPED));

                if (WSARecvFrom(fd, &recv_ctx.WSABuf, 1, &bytes, &flags, (sockaddr*)recv_ctx.address.data(), &addr_len
                    , (LPOVERLAPPED)&recv_ctx, nullptr) == SOCKET_ERROR)
                {
                    if (socket::get_error() != WSA_IO_PENDING)
                        socket::dump_error();
                }
#elif defined MTCP_OS_LINUX
                struct epoll_event ee;
                ee.data.ptr = this;
                ee.events = EPOLLIN | EPOLLET | EPOLLHUP;
                epoll_ctl(kernel, EPOLL_CTL_ADD, fd, &ee);

#endif

            }

            virtual void async_send(const string &addr, const char *data, size_t len)
            {
                ::sendto(fd, data, (int)len, 0, (struct sockaddr*)addr.data(), (int)addr.size());
            }

            void close(){
                if (type == session_t::e_server) {
                    for (auto iter : *server_sessions.get())
                    {
                        timer->cancel_timer(iter.second->timer_id);
                        iter.second->reset();
                        delete iter.second.get();
                    }

                    server_sessions.reset();
                }
                else{
                    timer->cancel_timer(client_session->timer_id);
                    client_session->callback->on_close(client_session.get());
                    client_session->reset();
                    client_session.reset();
                }
            }

            void on_message(const string &addr, const char *data, size_t len) {
                //std::cout << "on_message len:" << len << std::endl;
                protocol::message_t * message = new protocol::message_t(0);
                message->timestamp = get_timestamp();
                if (!message->decode(data, len)) {
                    std::cout << "message decode failed." << std::endl;
                    return;
                }

                session_t *session = nullptr;
                if (type == session_t::e_server) {
                    auto iter = server_sessions->find(addr);
                    if (iter == server_sessions->end()) {
                        if (!message->head.flags.sync)
                            return;

                        shared_ptr<session_t> sess = std::make_shared<session_t>(token, callback);
                        sess->peer = this;
                        sess->delay_close_queue = delay_close_queue;
                        sess->fd = fd;
                        session = sess.get();
                        session->timer_id = timer->set_timer(50, std::bind(&session_t::on_timer, session));
                        session->remote_address = addr;
                        socket::get_local_addr(fd, session->local_addr_info);
                        sockaddr_in *sa = (sockaddr_in*)addr.data();
                        session->remote_addr_info.ip = inet_ntoa(sa->sin_addr);
                        session->remote_addr_info.port = ntohs(sa->sin_port);
                        server_sessions->emplace(addr, std::move(sess));
                    }
                    else{
                        session = iter->second.get();
                    }
                }
                else if (type == session_t::e_client) {
                    session = client_session.get();
                }
                else {
                    return;
                }

                session->recv_timestamp = message->timestamp;

                if (message->head.flags.sync && session->remote_token != message->head.token)
                {
                    session->reset();
                    session->remote_token = message->head.token;
                    session->peer->callback->on_open(session);
                    if (type == session_t::e_server)
                        session->send_sync();
                    return;
                }
                
                if (message->head.flags.fin)
                {
                    session->peer->callback->on_close(session);
                    if (type == session_t::e_server){
                        server_sessions->erase(addr);
                        session->close(false);
                        delete session;
                    }
                    else
                    {
                        session->close(false);
                    }
                    return;
                }



                session->on_recv(message);
            }

        protected:
            string name;
            int type;
            std::shared_ptr<std::unordered_map<std::string, shared_ptr<session_t> > > server_sessions;
            shared_ptr<session_t> client_session;
            mtcp_socket_t fd = mtcp_invalid_socket;
            unsigned int token = 0;
            unsigned int recv_timeout = 0;
#if (defined MTCP_OS_WINDOWS)
            HANDLE kernel = nullptr;
#elif defined MTCP_OS_LINUX
            mtcp_socket_t kernel = mtcp_invalid_socket;
#endif
        public:
            timer_manager *timer = nullptr;
            void * bind_data = nullptr;
            callback_t *callback = nullptr;
            context recv_ctx;
            delay_close_queue_t *delay_close_queue = nullptr;
        };

        class proactor_engine{
        public:
            proactor_engine() {
#ifdef MTCP_OS_WINDOWS
                socket::init();
                _kernel = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
#elif defined MTCP_OS_LINUX
                _kernel = epoll_create(64);
#endif
            }
            ~proactor_engine(){
#ifdef MTCP_OS_WINDOWS
#elif defined MTCP_OS_LINUX
                socket::close(_kernel);
#endif
            }
        private:
        public:

            void run() {
#ifdef MTCP_OS_WINDOWS

                while (true)
                {
                    if (_stop)
                        return close_all();

                    DWORD timeout = (DWORD)_timer_manager.do_timer();

                    void *key = nullptr;
                    context *ctx = nullptr;
                    DWORD len = 0;
                    BOOL rc = GetQueuedCompletionStatus(_kernel, &len, (PULONG_PTR)&key, (LPOVERLAPPED*)&ctx,
                        timeout > 0 ? timeout : 1000);

                    if (rc == FALSE)
                    {
                        if (GetLastError() == WAIT_TIMEOUT)
                            continue;
                    }

                    if (ctx == nullptr)
                        continue;

                    if (ctx->status == context::e_recv) {
                        if (len == 0)
                        {
                            socket::dump_error();
                            ctx->peer->close();
                            delete ctx->peer;
                        }
                        else{
                            ctx->peer->on_message(ctx->address, ctx->buf.data(), (size_t)len);
                            ctx->peer->async_recv();
                        }
                    }

                }
#else
                struct sigaction action;
                action.sa_flags = 0;
                action.sa_handler = mtcp_handle_pipe;

                sigaction(SIGPIPE, &action, NULL);

                const int max_event_size = 64;
                while (!_stop)
                {
                    std::size_t timeout = _timer_manager.do_timer();

                    struct epoll_event event_arr[max_event_size];

                    int num = epoll_wait(_kernel, event_arr, max_event_size, timeout > 0 ? timeout : 1000);

                    for (int i = 0; i < num; i++)
                    {
                        struct epoll_event &ee = event_arr[i];
                        peer_t *peer = (peer_t*)ee.data.ptr;

                        if (ee.events & (EPOLLERR | EPOLLHUP))
                        ;
                            //peer->close();
                        else
                        {
                            if (ee.events & EPOLLIN)
                            {
                                for (;;)
                                {
                                    socklen_t addr_len = peer->recv_ctx.address.size();
                                    
                                    int recv_len = ::recvfrom(peer->fd, (void*)peer->recv_ctx.buf.data(), peer->recv_ctx.buf.size(), 0, (struct sockaddr*)peer->recv_ctx.address.data(), &addr_len);
                                    
                                    if (recv_len > 0)
                                    {                              
                                        peer->on_message(peer->recv_ctx.address, peer->recv_ctx.buf.data(), (size_t)recv_len);
//                                         ctx->peer->async_recv();
                                    }
                                    else if (recv_len < 0)
                                    {
                                        int err = socket::get_error();
                                        if (err != EWOULDBLOCK && err != EAGAIN && err != EINTR)
                                        {
                                            //close();
                                        }

                                        break;
                                    }
                                    else if (recv_len == 0)
                                    {
                                        break;
                                        //close();
                                    }
                                }
                            }
                        }
                    }
                }
#endif
            }

            void stop() {
                _stop = true;
            }

            session_t *add_client(const string &remote_addr, const string &local_addr, callback_t *cb) {
                peer_t *peer = add_peer(false, remote_addr, local_addr, cb);
                if (peer != nullptr)
                    return peer->client_session.get();
                return nullptr;
            }

            bool add_server(const string &bind_addr, callback_t *cb) {
                return add_peer(true, "", bind_addr, cb) != nullptr;
            }

    private:
        void close_all(){
#ifdef MTCP_OS_WINDOWS
            CloseHandle(_kernel);
#else
            socket::close(_kernel);
#endif // MTCP_OS_WINDOWS

            for (peer_t *peer : _peers)
            {
                peer->close();
                delete peer;
            }

            _peers.clear();
        }

        peer_t *add_peer(bool server, const string &remote_addr, const string &local_addr, callback_t *cb){
#ifdef MTCP_OS_WINDOWS
            if (_kernel == nullptr)
                return false;
#elif defined MTCP_OS_LINUX
            if (_kernel == mtcp_invalid_socket)
                return false;
#endif
            mtcp_socket_t fd = socket::create(AF_INET, SOCK_DGRAM, 0);
            if (fd == mtcp_invalid_socket)
                return false;

#ifdef MTCP_OS_WINDOWS
            BOOL opt = FALSE;
            DWORD dw = 0;
            WSAIoctl(fd, SIO_UDP_CONNRESET, &opt, sizeof(opt), NULL, 0, &dw, NULL, NULL);
#endif


            socket::set_noblock(fd);

            address_info_t remote_info, local_info;
            peer_t *peer = new peer_t;
            peer->type = server ? session_t::e_server : session_t::e_client;
            peer->fd = fd;
            peer->timer = &_timer_manager;
            peer->kernel = _kernel;
            peer->callback = cb;
            peer->delay_close_queue = &_delay_close_queue;
            peer->token = get_timestamp() & 0xFFFFFFFF;


            if (peer->type == session_t::e_client) {
                peer->client_session = std::make_shared<session_t>(peer->token, cb);
                peer->client_session->peer = peer;
                peer->client_session->delay_close_queue = peer->delay_close_queue;
                peer->client_session->type = session_t::e_client;
                peer->client_session->fd = fd;
                session_t *session = peer->client_session.get();
                session->timer_id = _timer_manager.set_timer(20, std::bind(&session_t::on_timer, session));
            }

            {
                if (server) {
                    if (!socket::bind(fd, local_addr))
                    {
                        socket::dump_error();
                        goto ON_FAILED;
                   }

                    peer->server_sessions = std::make_shared<std::unordered_map<std::string, shared_ptr<session_t> > >();

                    struct sockaddr_in * sock = (sockaddr_in*)peer->recv_ctx.address.data();
                    sock->sin_family = AF_INET;
                    sock->sin_port = htons(local_info.port);
                    sock->sin_addr.s_addr = inet_addr(local_info.ip.c_str());
                }
                else {
                    if (!socket::parse_addr(remote_addr, remote_info))
                        goto ON_FAILED;

                    struct sockaddr_in * sock = (sockaddr_in*)peer->recv_ctx.address.data();
                    sock->sin_family = AF_INET;
                    sock->sin_port = htons(remote_info.port);
                    sock->sin_addr.s_addr = inet_addr(remote_info.ip.c_str());

                    if (!socket::connect(fd, (struct sockaddr*)sock))
                        socket::dump_error();

                    peer->client_session->local_addr_info = local_info;
                    peer->client_session->remote_addr_info = remote_info;
                    peer->client_session->remote_address = peer->recv_ctx.address;
                    if (!local_addr.empty() && !socket::bind(fd, local_addr))
                        goto ON_FAILED;

                    peer->client_session->send_sync();
//                     peer->active_callback(peer->client_session.get());
                }


#ifdef MTCP_OS_WINDOWS
                DWORD dw = 0;
                BOOL opt = FALSE;

                if (WSAIoctl(fd, SIO_UDP_CONNRESET, &opt, sizeof(opt), NULL, 0, &dw, NULL, NULL) == SOCKET_ERROR)
                {
                    if (WSAEWOULDBLOCK != socket::get_error())
                        goto ON_FAILED;
                }

                if (CreateIoCompletionPort((HANDLE)fd, _kernel, 0, 0) != _kernel)
                    goto ON_FAILED;
#endif
                peer->async_recv();
            }

            _peers.insert(peer);
            return peer;
        ON_FAILED:
            delete peer;
            socket::close(fd);
            return nullptr;
        }

        private:
#ifdef MTCP_OS_WINDOWS
            HANDLE _kernel = nullptr;///iocp
#elif defined MTCP_OS_LINUX
            mtcp_socket_t _kernel;//epoll fd
#endif
            bool _stop = false;
            timer_manager _timer_manager;
            std::set<peer_t*> _peers;
            std::deque<session_t*> _delay_close_queue;
        };

}
