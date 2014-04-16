/* Local slave reader control: spawning and killing.
 *
 * Local slave readers are read only slaves forked by the main Redis server.
 * Readers are forked to exploit the multiple CPU cores for reading requests,
 * which may be much more than writing requests, and can sometimes tolerate some
 * stale data.
 *
 * The readers are forked periodically and never replicate from the master to
 * exploit the copy-on-write semantic of fork. Not to be left behind for too
 * long, the readers are periodically killed and respawned.
 *
 * Copyright (c) 2014, Zhang Yichao <echaozh at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"
#include "reader.h"

#include <signal.h>
#include <sys/wait.h>

static void resetReaderParams(void) {
    server.reader_count = 0;
}

static void setupAsSlave(void) {
    /* First unset current master if there's any. */
    replicationUnsetMaster();
    /* Disconnect connected slaves. */
    disconnectSlaves();

    server.masterhost = "";     /* So long as it is not NULL */
    server.repl_serve_stale_data = 1;
    server.repl_slave_ro = 1;
    server.repl_slave_reader = 1;
}

static int readerSpawnOne(void) {
    pid_t childpid;
    long long start;

    start = ustime();
    if ((childpid = fork()) == 0) {
        /* Child */
        closeListeningSockets(0);
        /* Prevent reader from saving rdb in the background. */
        resetServerSaveParams();
        server.rdb_child_pid = -1;
        /* Disable aof. */
        resetAppendOnly();
        /* Reader should not spawn more readers. */
        resetReaderParams();
        /* Act as a read-only slave which serves stale data, and never connect
         * to the master */
        setupAsSlave();
        disconnectClients();

        /* Readers are not part of cluster. */
        server.cluster_enabled = 0;
        /* Sentinels don't need readers. */
        redisAssert(!server.sentinel_mode);

        redisSetProcTitle("redis-local-reader");
        return REDIS_OK;
    } else {
        /* Parent */
        server.stat_fork_time = ustime()-start;
        if (childpid == -1) {
            redisLog(REDIS_WARNING,"Can't spawn local readers: fork: %s",
                strerror(errno));
            return REDIS_ERR;
        }
        redisLog(REDIS_VERBOSE,"Local reader spawn as pid %d", childpid);
        if (!listAddNodeHead(server.readers,(void*)(ptrdiff_t)childpid)) {
            int statloc;

            redisLog(REDIS_WARNING,"No memory to reference reader with pid %d",
                childpid);
            if (kill(childpid,SIGKILL) != -1)
                wait3(&statloc,childpid,NULL);
            else {
                redisLog(REDIS_WARNING,"failed to kill reader with pid %d",
                    childpid);
            }
            return REDIS_ERR;
        }
        return REDIS_OK;
    }
}

void readerSpawn(void) {
    int i = listLength(server.readers);

    if(server.reader_count <= i)
        return;

    for (; i < server.reader_count; i++)
        readerSpawnOne();

    /* Even if not all readers are up, we can update states
     * so we can later check if the up readers need to be killed or not on
     * retry */
    server.reader_dirty = 0;
    server.last_reader_spawn = time(NULL);
}

void readerKill() {
    listNode *ln;
    listIter li;

    listRewind(server.readers,&li);
    while((ln = listNext(&li))) {
        pid_t reader = (pid_t)(ptrdiff_t)listNodeValue(ln);

        if (kill(reader,SIGKILL) != -1)
            redisLog(REDIS_VERBOSE,"killing local reader with pid %d", reader);
        else {
            redisLog(REDIS_WARNING,"failed to kill local reader with pid %d",
                reader);
            listDelNode(server.readers,ln);
        }
    }

    listRewind(server.readers,&li);
    while((ln = listNext(&li))) {
        int statloc;
        pid_t reader = (pid_t)(ptrdiff_t)listNodeValue(ln);

        wait3(&statloc,reader,NULL);
        listDelNode(server.readers,ln);
    }
}

int readerExitHandler(pid_t pid, int exitcode, int bysignal) {
    listNode *ln;

    if ((ln = listSearchKey(server.readers,(void*)(ptrdiff_t)pid))) {
        if (!bysignal) {
            redisLog(REDIS_WARNING,"reader with pid %d exited with code %d",
                pid, exitcode);
        } else {
            redisLog(REDIS_WARNING,"reader with pid %d killed by signal %d",
                pid, bysignal);
        }
        listDelNode(server.readers,ln);
        readerSpawnOne();
        return 1;
    } else
        return 0;
}
