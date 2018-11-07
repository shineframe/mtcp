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
typedef int mtcp_socket_t;
#define mtcp_invalid_socket -1

void mtcp_handle_pipe(int sig) {}

#endif

#include <iostream>
#include <chrono>
#include <ctime>
#include <string>
#include <mutex>
#include <map>
#include <vector>
#include <forward_list>
#include <functional>

#define mtcp_invalid_timer_id 0

namespace mtcp
{
        using namespace std;
        struct address_info_t{
            string ip;
            unsigned short port;
        };

        typedef std::function<bool()> timer_callback_t;

        static unsigned long long get_timestamp()
        {
            auto now = std::chrono::high_resolution_clock::now();
            return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        }

        struct timer_t
        {
            unsigned long long id;
            unsigned long long delay;
            unsigned long long timestamp;
            timer_callback_t callback;
        };

        class timer_manager : std::multimap <unsigned long long, timer_t>{
        public:
            unsigned long long do_timer() {
                if (empty())
                    return 0;
                unsigned long long now = get_timestamp();

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

            unsigned long long set_timer(unsigned long long delay, timer_callback_t cb) {
                timer_t item;
                item.id = ++_id;
                item.delay = delay;
                item.timestamp = get_timestamp() + delay;
                item.callback = std::move(cb);

                auto ret = item.id;
                emplace(item.timestamp, std::move(item));
                return ret;
            }

            bool cancel_timer(unsigned long long id) {
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
            unsigned long long _id = 0;
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

        struct peer_t;
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
                buf.resize(1024);
#if (defined MTCP_OS_WINDOWS)
                memset(this, 0, sizeof(OVERLAPPED));
                address.resize(sizeof(sockaddr_in));
                WSABuf.buf = (CHAR*)buf.data();
                WSABuf.len = (ULONG)buf.size();
#endif
            }

            void flush(size_t len) {
                buf.erase(0, len);
            }

            int status = e_recv;
            string buf;

#if (defined MTCP_OS_WINDOWS)
            WSABUF WSABuf;
            string address;
#endif
            peer_t *peer;
        };

        struct iovec_t {
            const char *data;
            size_t size;
        };

        class proactor_engine;
        struct peer_t;
        typedef std::function<bool(bool, peer_t *peer)> peer_callback_t;

        struct peer_t {
            friend class proactor_engine;
        public:
            peer_t(){
                recv_ctx.status = context::e_recv;
                recv_ctx.peer = this;
                send_ctx.status = context::e_send;
                send_ctx.peer = this;
            }
            virtual ~peer_t(){}

            virtual void async_recv()
            {
                DWORD bytes = 0;
                DWORD flags = 0;
                int addr_len = (int)recv_ctx.address.size();
                memset((void*)recv_ctx.address.data(), 0, recv_ctx.address.size());
                memset(&recv_ctx, 0, sizeof(OVERLAPPED));

                if (WSARecvFrom(fd, &recv_ctx.WSABuf, 1, &bytes, &flags, (sockaddr*)recv_ctx.address.data(), &addr_len
                    , (LPOVERLAPPED)&recv_ctx, nullptr) == SOCKET_ERROR)
                {
                    
                    if (socket::get_error() != WSA_IO_PENDING)
                        std::cout << socket::get_error() << endl;
                }
            }

            virtual void async_send(const string &addr, const char *data, size_t len, bool flush = true)
            {
                if (len == 0)
                    return;

                DWORD bytes = 0;
                context &ctx = *pop_send_ctx();
                ctx.WSABuf.buf = (CHAR*)data;
                ctx.WSABuf.len = (ULONG)len;

                if (WSASendTo(fd, &ctx.WSABuf, 1, &bytes, 0, (sockaddr*)addr.data(), (int)addr.size(), (LPOVERLAPPED)&ctx, nullptr) == SOCKET_ERROR)
                {
                    if (WSAGetLastError() != WSA_IO_PENDING)
                    {
                        std::cout << socket::get_error() << endl;
                    }
                }
            }

        private:
            context * pop_send_ctx() {
                static const size_t chunk = 32;
                if (idle_pool.empty())
                {
                    send_pool.resize(send_pool.size() + chunk);
                    for (size_t i = send_pool.size() - chunk ; i < send_pool.size(); i++)
                    {
                        send_pool[i].status = context::e_send;
                        send_pool[i].peer = this;

                        if (i == send_pool.size() - 1)
                            return &send_pool[i];

                        idle_pool.push_front(&send_pool[i]);
                   }
                }

                context *ret = idle_pool.front();
                idle_pool.pop_front();
                return ret;
            }

            void push_send_ctx(context *ctx) {
                idle_pool.push_front(ctx);
            }
        protected:
            string name;
            mtcp_socket_t fd = mtcp_invalid_socket;
            unsigned int recv_timeout = 0;
#if (defined MTCP_OS_WINDOWS)
            HANDLE kernel = nullptr;
            std::vector<context> send_pool;
            std::forward_list<context*> idle_pool;
#elif defined MTCP_OS_LINUX
            mtcp_socket_t kernel = mtcp_invalid_socket;
#endif
        public:
            timer_manager *timer = nullptr;
            void * bind_data = nullptr;
            peer_callback_t peer_callback = nullptr;
            context recv_ctx;
            context send_ctx;
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

                    if (ctx->status == context::e_exit)
                    {
                        delete ctx;
                        break;
                    }

                    if (ctx == nullptr)
                        continue;

                    if (ctx->status == context::e_recv) {
                        ctx->peer->async_send(ctx->address, ctx->buf.data(), (size_t)len);

//                         sendto(ctx->peer->fd, ctx->buf.data(), (size_t)len, 0, (const struct sockaddr*)ctx->remote_address.data(), (int)ctx->remote_address.size());
                        ctx->peer->async_recv();
                    }
                    else if (ctx->status == context::e_send) {
                        ctx->peer->push_send_ctx(ctx);
                    }

                }
#else
                struct sigaction action;
                action.sa_flags = 0;
                action.sa_handler = shine_handle_pipe;

                sigaction(SIGPIPE, &action, NULL);

                const int max_event_size = 64;
                while (!_stop)
                {
                    unsigned long long timeout = _timer_manager.do_timer();

                    struct epoll_event event_arr[max_event_size];

                    int num = epoll_wait(_epoll_fd, event_arr, max_event_size, timeout > 0 ? timeout : 1000);

                    for (int i = 0; i < num; i++)
                    {
                        struct epoll_event &ee = event_arr[i];
                        peer_t *peer = (peer_t*)ee.data.ptr;

                        if (ee.events & (EPOLLERR | EPOLLHUP))
                            //peer->close();
                        else
                        {
                            if (ee.events & EPOLLIN)
                            {
                                for (;;)
                                {
                                    int addr_len = peer->recv_ctx.address.size();
                                    int recv_len = recvfrom(peer->fd, peer->recv_ctx.buf.data(), peer->recv_ctx.buf.size(), (struct sockaddr*)peer->recv_ctx.address, &addr_len);

                                    if (recv_len > 0)
                                    {

                                    }
                                    else if (recv_len < 0)
                                    {
                                        int err = socket::get_error();
                                        if (err != EWOULDBLOCK && err != EAGAIN && err != EINTR)
                                        {
                                            //close();
                                        }
                                    }
                                    else if (recv_len == 0)
                                    {
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

            bool add_peer(bool server, const string &name, const string &addr, peer_callback_t cb){
                if (_kernel == nullptr)
                    return false;

                address_info_t info;
                if (!socket::parse_addr(addr, info))
                    return false;

                mtcp_socket_t fd = socket::create(AF_INET, SOCK_DGRAM, 0);
                if (fd == mtcp_invalid_socket)
                    return false;

                if (server) {
                    if (!socket::bind(fd, addr))
                    {
                        socket::close(fd);
                        return false;
                    }
                }
                socket::set_noblock(fd);

#ifdef MTCP_OS_WINDOWS
                DWORD dw = 0;
                BOOL opt = FALSE;

                if (WSAIoctl(fd, SIO_UDP_CONNRESET, &opt, sizeof(opt), NULL, 0, &dw, NULL, NULL) == SOCKET_ERROR)
                {
                    if (WSAEWOULDBLOCK != socket::get_error())
                    {
                        socket::close(fd);
                        return false;
                    }
                }

                if (CreateIoCompletionPort((HANDLE)fd, _kernel, 0, 0) != _kernel)
                {
                    socket::close(fd);
                    return false;
                }
#endif

                peer_t *peer = new peer_t;
                peer->fd = fd;
                peer->timer = &_timer_manager;
                peer->kernel = _kernel;
                peer->peer_callback = std::move(cb);
                peer->async_recv();

#ifdef MTCP_OS_LINUX
                struct epoll_event ee;
                ee.data.ptr = peer;
                ee.events = EPOLLIN | EPOLLET | EPOLLHUP;
                epoll_ctl(_kernel, EPOLL_CTL_ADD, fd, &ee);
#endif
                return true;
            }

            bool add_client(const string &name, const string &addr, peer_callback_t cb) {
                return add_peer(false, name, addr, std::move(cb));
            }

            bool add_server(const string &name, const string &addr, peer_callback_t cb) {
                return add_peer(true, name, addr, std::move(cb));
            }

        private:
#ifdef MTCP_OS_WINDOWS
            HANDLE _kernel;///<Íê³É¶Ë¿Ú¾ä±ú
#elif defined MTCP_OS_LINUX
            mtcp_socket_t _kernel;//epoll fd
#endif
            bool _stop = false;
            timer_manager _timer_manager;
        };

}
