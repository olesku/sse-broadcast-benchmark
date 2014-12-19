/*
 * Example implementation of SSE-Server according to criterias specified by
 * https://github.com/rexxars/sse-broadcast-benchmark
 *
 * Written by Ole Fredrik Skudsvik <oles@vg.no>, Dec 2014
 *
 * */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <pthread.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <sys/epoll.h>

/* Defaults. */
#define LISTEN_PORT 1942
#define NUM_THREADS 4
#define LOGLEVEL LOG_DEBUG

#define MAXEVENTS 1024

#define HTTP_200 "HTTP/1.1 200 OK\r\n"
#define HTTP_204 "HTTP/1.1 204 No Content\r\n"
#define HTTP_404 "HTTP/1.1 404 Not Found\r\n"
#define HTTP_400 "HTTP/1.1 400 Bad Request\r\n"


#define RESPONSE_HEADER_STREAM      "Content-Type: text/event-stream\r\n" \
                                    "Cache-Control: no-cache\r\n" \
                                    "Access-Control-Allow-Origin: *\r\n" \
                                    "Connection: keep-alive\r\n\r\n"

#define RESPONSE_HEADER_PLAIN       "Content-Type: text/plain\r\n" \
                                    "Cache-Control: no-cache\r\n"  \
                                    "Access-Control-Allow-Origin: *\r\n" \
                                    "Connection: close\r\n\r\n"

#define RESPONSE_OPTIONS_CORS       "Access-Control-Allow-Origin: *\r\n" \
                                    "Connection: close\r\n\r\n"
#define MAXBCAST_SIZE 8096

typedef struct {
  pthread_t thread;
  int id;
  int epollfd;
  int numclients;
} ssethread_t;


typedef enum  {
  LOG_INFO,
  LOG_ERROR,
  LOG_DEBUG
} loglevel_t;


int efd;
struct epoll_event *events;

ssethread_t **threadpool;
pthread_t t_main, t_timefeed;

pthread_mutex_t logmutex;
pthread_mutex_t bcast_mutex;
pthread_mutex_t do_ssebroadcast_mutex;

pthread_cond_t do_ssebroadcast_cond;

char bcast_buf[MAXBCAST_SIZE];

void writelog(loglevel_t level, const char* fmt, ...) {
  va_list arglist;
  time_t ltime;
  struct tm result;
  char stime[32];

  if (level == LOG_DEBUG && LOGLEVEL != LOG_DEBUG) return;
  if (level == LOG_INFO && LOGLEVEL == LOG_ERROR)  return;

  ltime = time(NULL);
  localtime_r(&ltime, &result);
  asctime_r(&result, stime);
  stime[strlen(stime)-1] = '\0';

  pthread_mutex_lock(&logmutex);
  va_start(arglist, fmt);
  printf("%s ", stime);
  vprintf(fmt, arglist);
  printf("\n");
  va_end(arglist);
  pthread_mutex_unlock(&logmutex);
}

int writesock(int fd, const char* fmt, ...) {
  char buf[8096];
  va_list arglist;

  va_start(arglist, fmt);
  vsnprintf(buf, 8096, fmt, arglist);
  va_end(arglist);

  return send(fd, buf, strlen(buf), 0);
}

/*
 * Simple parser to extract uri from GET requests.
 * */
int get_uri(char* str, int len, char *dst, int bufsiz) {
  int i;

  if (strncmp("GET ", str, 4) == 0) {
    for(i=4; i < len && str[i] != ' ' && i < bufsiz; i++) {
      *dst++ = str[i];
    }
    *(dst) = '\0';
    dst -= i;

    return 1;
  }

  return 0;
}

int getnumclients() {
  int i;
  int count = 0;

  for (i = 0; i < NUM_THREADS; i++) {
    count += threadpool[i]->numclients;
  }

  return count;
}

int write_bcast(char *fmt, ...) {
  char buf[MAXBCAST_SIZE];
  int len;
  va_list arglist;

  va_start(arglist, fmt);
  
  vsnprintf(buf, MAXBCAST_SIZE, fmt, arglist);
  len = strlen(buf);

  pthread_mutex_lock(&do_ssebroadcast_mutex);
  memcpy(&bcast_buf, buf, len);
  bcast_buf[len] = '\0';
  pthread_cond_broadcast(&do_ssebroadcast_cond); 
  pthread_mutex_unlock(&do_ssebroadcast_mutex);
 
  va_end(arglist);
  
  return len;
}

char* get_bcast() {
  return bcast_buf;
}

/*
 * Broadcasts data: timestamp every second.
 */
void *timestamp_feeder() {
  struct timeval tv;

  while (1) {
    gettimeofday(&tv, NULL);
    write_bcast("data: %ld\n\n", tv.tv_sec*1000LL + tv.tv_usec/1000);
    sleep(1);
  }
}

/*
 * SSE channel handler.
 */
void *sse_thread(void* _this) {
 int t_maxevents, n, i;
 struct epoll_event *t_events, t_event;
 ssethread_t* this = (ssethread_t*)_this;

 t_maxevents = sysconf(_SC_OPEN_MAX) / NUM_THREADS;
 t_events = calloc(t_maxevents, sizeof(t_event));
 this->numclients = 0;

  while(1) {
    pthread_cond_wait(&do_ssebroadcast_cond, &do_ssebroadcast_mutex);

    n = epoll_wait(this->epollfd, t_events, t_maxevents, 0);
    for (i = 0; i < n; i++) {
      if ((t_events[i].events & EPOLLHUP) || (t_events[i].events & EPOLLRDHUP)) {
        writelog(LOG_DEBUG, "sse_thread #%i: Client disconnected.", this->id);
        close(t_events[i].data.fd);
        this->numclients--;
        continue;
      }

      if (t_events[i].events & EPOLLERR) {
        writelog(LOG_ERROR, "sse_thread #%i: Socket error: %s.", this->id, strerror(errno));
        continue;
      }

       writesock(t_events[i].data.fd, "%s", get_bcast());
    }

    if (i > 0)  writelog(LOG_DEBUG, "Thread #%i: broadcasted to %i clients.", this->id, i);
  }
}

void post_broadcast_handler(int fd, char* buf) {
  char *p, data[512];
  int i;
  p = strstr(buf, "data=");
  
  if (p == NULL) {
    writesock(fd, "%sConnection: close\r\n\r\nInvalid broadcast request.", HTTP_400); 
    return;
  }

  p+=5;

  for (i=0; *p != '\0' && i < 512; p++, i++) {
    data[i] = *p;
  }

  data[i] = '\0';

  write_bcast("%s\n", data);
  writesock(fd, "%sConnection: close\r\n\r\nBroadcasted '%s' to %i clients.\n", HTTP_200, data, getnumclients());
}

/*
 * Main handler.
 * */
void *main_thread() {
  int i, n, len, ret, cur_thread;
  char buf[512], uri[64];
  struct epoll_event event;

  cur_thread = 0;

  while(1) {
    n = epoll_wait(efd, events, MAXEVENTS, -1);

    for (i = 0; i < n; i++) {
      if ((events[i].events & EPOLLHUP) || (events[i].events & EPOLLRDHUP)) {
        close(events[i].data.fd);
        continue;
      }

      if ((events[i].events & EPOLLERR) || (!(events[i].events & EPOLLIN))) {
        writelog(LOG_DEBUG, "main_thread(): Error occurred while reading data from socket.");
        continue;
      }

      /* Read from client. */
      len = read(events[i].data.fd, &buf, 512);
      buf[len] = '\0';

      if (strncmp(buf, "OPTIONS", 7) == 0) {
          writesock(events[i].data.fd, "%s%s", HTTP_204, RESPONSE_OPTIONS_CORS);
          close(events[i].data.fd);
          continue;
      }

      if (strncmp(buf, "POST /broadcast", 15) == 0) {
          post_broadcast_handler(events[i].data.fd, buf);
          close(events[i].data.fd);
          continue;
      }


      if (get_uri(buf, len, uri, 64)) {
        writelog(LOG_DEBUG, "GET %s.", uri);

        if (strcmp(uri, "/connections") == 0) { // Handle /connections.
          writesock(events[i].data.fd, "%s%s%i", HTTP_200, RESPONSE_HEADER_PLAIN, getnumclients());
        } else if (strcmp(uri, "/sse") == 0) { // Handle SSE channel.
          writesock(events[i].data.fd, "%s%s:ok\n\n", HTTP_200, RESPONSE_HEADER_STREAM, uri);

           /* Add client to the epoll eventlist. */
          event.data.fd = events[i].data.fd;
          event.events = EPOLLOUT;

          ret = epoll_ctl(threadpool[cur_thread]->epollfd, EPOLL_CTL_ADD, events[i].data.fd, &event);
          if (ret == -1) {
            writelog(LOG_ERROR, "main_thread: Could not add client to epoll eventlist (curthread: %i): %s.", cur_thread, strerror(errno));
            close(events[i].data.fd);
            continue;
          }

          /* Remove fd from main efd set. */
          epoll_ctl(efd, EPOLL_CTL_DEL, events[i].data.fd, NULL);

          /* Increment numclients and cur_thread. */
          threadpool[cur_thread]->numclients++;
          cur_thread = (cur_thread == NUM_THREADS-1) ? 0 : (cur_thread+1);

          continue; /* We should not close the connection. */
        } else { // Return 404 on everything else.
          writesock(events[i].data.fd, "%s%sNo such channel.\r\n", HTTP_404, RESPONSE_HEADER_PLAIN);
        }
      }

      close(events[i].data.fd);
    }
  }
}

void cleanup() {
  int i;
  for (i = 0; i<NUM_THREADS; close(threadpool[i]->epollfd), free(threadpool[i]));
  free(threadpool);
  free(events);
}

int main(int argc, char *argv[]) {
  int serversock, ret, on, tmpfd, i;
  struct sockaddr_in sin;

  on = 1;

  /* Ignore SIGPIPE. */
  signal(SIGPIPE, SIG_IGN);

  /* Set up listening socket. */
  serversock = socket(AF_INET, SOCK_STREAM, 0);
  if (serversock == -1) {
    writelog(LOG_ERROR, "Creating socket: %s.", strerror(errno));
    exit(1);
  }

  setsockopt(serversock, SOL_SOCKET, SO_REUSEADDR, (const char*)&on, sizeof(on));

  memset((char*)&sin, '\0', sizeof(sin));
  sin.sin_family  = AF_INET;
  sin.sin_port  = htons(LISTEN_PORT);


  ret = bind(serversock, (struct sockaddr*)&sin, sizeof(sin));
  if (ret == -1) {
    writelog(LOG_ERROR, "bind(): %s", strerror(errno));
    exit(1);
  }

  ret = listen(serversock, 0);
   if (ret == -1) {
    writelog(LOG_ERROR, "listen(): %s", strerror(errno));
    exit(1);
  }

  writelog(LOG_INFO, "Listening on port %i.", LISTEN_PORT);

  /* Initial client handler thread. */
  ret = pthread_create(&t_main, NULL, main_thread, NULL);

  /* t_bcast */
  ret = pthread_create(&t_timefeed, NULL, timestamp_feeder, NULL);
  
  /* Set up SSE worker threads. */
  threadpool = (ssethread_t**)malloc(NUM_THREADS  * sizeof(ssethread_t*));
  for (i=0; i <= NUM_THREADS; threadpool[i++] = (ssethread_t*)malloc(sizeof(ssethread_t)));

  writelog(LOG_INFO, "Starting %i worker threads.", NUM_THREADS);
  for (i = 0; i < NUM_THREADS; i++) {
    threadpool[i]->id = i;
    threadpool[i]->epollfd = epoll_create1(0);

    if (threadpool[i]->epollfd == -1) {
      writelog(LOG_ERROR, "Failed to create epoll descriptor for thread #%i: %s.", i,  strerror(errno));
      cleanup();
      exit(1);
    }

    pthread_create(&threadpool[i]->thread, NULL, sse_thread, threadpool[i]);
   }

  /* Set up epoll stuff. */
  efd = epoll_create1(0);
  if (efd == -1) {
    writelog(LOG_ERROR, "Failed to create epoll descriptor: %s.", strerror(errno));
    exit(1);
  }

  events = calloc(MAXEVENTS, sizeof(struct epoll_event));

  /* Mainloop, clients will be accepted here. */
  while(1) {
    struct sockaddr csin;
    socklen_t clen;
    struct epoll_event event;

    clen = sizeof(csin);

    /* Accept the connection. */
    tmpfd = accept(serversock, (struct sockaddr*)&csin, &clen);

    /* Got an error ? Handle it. */
    if (tmpfd == -1) {
      switch (errno) {
        case EMFILE:
          writelog(LOG_ERROR, "All connections available used. Cannot accept more connections.");
          usleep(100000);
        break;

        default:
          writelog(LOG_ERROR, "Error in accept(): %s.", strerror(errno));
      }

      continue; /* Try again. */
    }

    /* If we got this far we've accepted the connection */
    fcntl(tmpfd, F_SETFL, O_RDWR | O_NONBLOCK); // Set non-blocking on the clientsocket.

    /* Add client to the epoll eventlist. */
    event.data.fd = tmpfd;
    event.events = EPOLLIN; // | EPOLLONESHOT;

    ret = epoll_ctl(efd, EPOLL_CTL_ADD, tmpfd, &event);
    if (ret == -1) {
      writelog(LOG_ERROR, "Could not add client to epoll eventlist: %s.", strerror(errno));
      close(tmpfd);
      continue;
    }

    writelog(LOG_DEBUG, "Accepted new connection.");
}

  cleanup();
  return(0);
}
