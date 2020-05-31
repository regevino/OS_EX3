//
// Created by Dan on 5/20/2020.
//

#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include <iostream>
#include <cmath>
#include <cassert>

#define SYS_ERR "system error: "

#define MUTEX_LOCK_ERR "mutex lock failed\n"

#define COND_WAIT_ERR "condition wait failed\n"

#define MUTEX_UNLOCK_ERR "mutex unlock failed\n"

#define COND_BROAD_ERR "conditional broadcast failed\n"

#define JOIN_ERR "join failed\n"

#define CREATE_ERR "create failed\n"

#define DESTRUCT_ERR "cond or mutex destroy failed\n"


static void mutex_lock_wrapper(pthread_mutex_t &mutex)
{
    if (pthread_mutex_lock(&mutex))
    {
        std::cerr << SYS_ERR << MUTEX_LOCK_ERR;
        exit(EXIT_FAILURE);
    }
}

static void mutex_unlock_wrapper(pthread_mutex_t &mutex)
{
    if (pthread_mutex_unlock(&mutex))
    {
        std::cerr << SYS_ERR << MUTEX_UNLOCK_ERR;
        exit(EXIT_FAILURE);
    }
}

class Job
{
public:
    class WorkerThread
    {
    private:
        std::vector<std::pair<K2 *, V2 *>> outputVector;
        Job &job;
        pthread_mutex_t mutex;
        bool isShuffleThread;
    public:
        pthread_t thread{};

        explicit WorkerThread(Job &job, bool isShuffle = false)
                : job(job), mutex(PTHREAD_MUTEX_INITIALIZER), isShuffleThread(isShuffle)
        {
        }

        ~WorkerThread()
        {
            if (pthread_mutex_destroy(&mutex))
            {
                std::cerr << SYS_ERR << DESTRUCT_ERR;
                exit(EXIT_FAILURE);
            }
        }

        void mapWork()
        {
            int oldIndex = (job.sharedIndex)++;
            while (oldIndex < (int) job.inputVec.size())
            {
                job.client.map(job.inputVec[oldIndex].first, job.inputVec[oldIndex].second, this);
                oldIndex = (job.sharedIndex)++;
            }
            mutex_lock_wrapper(job.shuffleWaitMutex);
            int previousNum = job.numOfMappingFinished++;
            if (previousNum == (int) job.workerThreads.size() - 1)
            {
                job.stage = stage_t::SHUFFLE_STAGE;
            }
            if (pthread_cond_wait(&(job.shufflePhase), &job.shuffleWaitMutex))
            {
                std::cerr << SYS_ERR << COND_WAIT_ERR;
                exit(EXIT_FAILURE);
            }
            mutex_unlock_wrapper(job.shuffleWaitMutex);
        }

        void shuffleWork()
        {
            bool moreToShuffle = true;
            while (job.stage == stage_t::MAP_STAGE || moreToShuffle)
            {
                moreToShuffle = false;
                for (auto &workerThread: job.workerThreads)
                {
                    mutex_lock_wrapper(workerThread.mutex);
                    while (!workerThread.outputVector.empty())
                    {
                        moreToShuffle = true;
                        auto output = workerThread.outputVector.back();
                        workerThread.outputVector.pop_back();
                        job.shuffleMap[output.first].push_back(output.second);
                        ++job.shuffledKeys;
                    }
                    mutex_unlock_wrapper(workerThread.mutex);
                }
            }
            std::transform(job.shuffleMap.begin(), job.shuffleMap.end(),
                           std::back_inserter(job.outputKeys),
                           [](const std::pair<K2 *const, std::vector<V2 *>> &pair) { return pair.first; });
            mutex_lock_wrapper(job.waitMutex);
            mutex_lock_wrapper(job.shuffleWaitMutex);
            job.stage = stage_t::REDUCE_STAGE;
            job.sharedIndex = 0;
            if (pthread_cond_broadcast(&(job.shufflePhase)))
            {
                std::cerr << SYS_ERR << COND_BROAD_ERR;
                exit(EXIT_FAILURE);
            }
            mutex_unlock_wrapper(job.shuffleWaitMutex);

        }

        void reduceWork()
        {
            int oldIndex = (job.sharedIndex)++;
            while (oldIndex < (int) job.outputKeys.size())
            {
                auto key = job.outputKeys[oldIndex];
                job.client.reduce(key, job.shuffleMap[key], this);
                oldIndex = (job.sharedIndex)++;
            }

            if (isShuffleThread)
            {
                for (auto &th: job.workerThreads)
                {
                    if (pthread_join(th.thread, nullptr))
                    {
                        std::cerr << SYS_ERR << JOIN_ERR;
                        exit(EXIT_FAILURE);
                    }
                }
                if (pthread_cond_broadcast(&job.jobDone))
                {
                    std::cerr << SYS_ERR << COND_BROAD_ERR;
                    exit(EXIT_FAILURE);
                }
                mutex_unlock_wrapper(job.waitMutex);
            }
        }

        void threadWork()
        {
            if (!isShuffleThread)
            {
                mapWork();
            }
            else
            {
                shuffleWork();
            }
            reduceWork();
        }

        void emit2(K2 *key, V2 *value)
        {
            mutex_lock_wrapper(mutex);
            outputVector.emplace_back(key, value);
            mutex_unlock_wrapper(mutex);
            ++job.mappedKeys;
        }

        void emit3(K3 *key, V3 *value)
        {
            mutex_lock_wrapper(job.outputVecMutex);
            job.outputVec.emplace_back(key, value);
            mutex_unlock_wrapper(job.outputVecMutex);
            ++job.reducedKeys;
        }

        static void *runThread(void *arg)
        {
            ((WorkerThread *) arg)->threadWork();
            return nullptr;
        }
    };

    Job(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
        int multiThreadLevel) : client(client), inputVec(inputVec), outputVec(outputVec),
                                workerThreads(multiThreadLevel - 1, WorkerThread(*this)),
                                shuffleThread(*this, true),
                                outputVecMutex(PTHREAD_MUTEX_INITIALIZER),
                                waitMutex(PTHREAD_MUTEX_INITIALIZER),
                                shuffleWaitMutex(PTHREAD_MUTEX_INITIALIZER),
                                shufflePhase(PTHREAD_COND_INITIALIZER),
                                jobDone(PTHREAD_COND_INITIALIZER),
                                sharedIndex(0),
                                numOfMappingFinished(0),
                                stage(stage_t::MAP_STAGE),
                                mappedKeys(0),
                                shuffledKeys(0),
                                reducedKeys(0)
    {
        for (auto &thread: workerThreads)
        {
            if (pthread_create(&thread.thread, nullptr, WorkerThread::runThread, &thread))
            {
                std::cerr << SYS_ERR << CREATE_ERR;
                exit(EXIT_FAILURE);
            }
        }
        if (pthread_create(&shuffleThread.thread, nullptr, WorkerThread::runThread, &shuffleThread))
        {
            std::cerr << SYS_ERR << CREATE_ERR;
            exit(EXIT_FAILURE);
        }
    }

    ~Job()
    {
        pthread_join(shuffleThread.thread, nullptr);
        if (pthread_cond_broadcast(&jobDone))
        {
            std::cerr << SYS_ERR << COND_BROAD_ERR;
            exit(EXIT_FAILURE);
        }
        if (pthread_mutex_destroy(&outputVecMutex)
            || pthread_mutex_destroy(&waitMutex)
            || pthread_cond_destroy(&shufflePhase)
            || pthread_cond_destroy(&jobDone))
        {
            std::cerr << SYS_ERR << DESTRUCT_ERR;
            exit(EXIT_FAILURE);
        }
    }

    void waitForJob()
    {
        JobState currentState;
        getJobState(&currentState);
        mutex_lock_wrapper(waitMutex);
        if (currentState.stage != stage_t::REDUCE_STAGE)
        {
            if (pthread_cond_wait(&jobDone, &waitMutex))
            {
                std::cerr << SYS_ERR << COND_WAIT_ERR;
                exit(EXIT_FAILURE);
            }
        }
        mutex_unlock_wrapper(waitMutex);
    }

    void getJobState(JobState *state)
    {
        int curSharedIndex = sharedIndex;
        state->stage = (stage_t) (int) stage;
        switch (state->stage)
        {
            case stage_t::MAP_STAGE:
                if ((int) inputVec.size() <= curSharedIndex)
                {
                    state->percentage = 100.0;
                    return;
                }
                state->percentage = 100 * (float) curSharedIndex / inputVec.size();
                return;
            case stage_t::SHUFFLE_STAGE:
                state->percentage = 100 * (float) shuffledKeys / mappedKeys;
                return;
            case stage_t::REDUCE_STAGE:
                state->percentage = 100 * (float) reducedKeys / shuffleMap.size();
                return;
            case stage_t::UNDEFINED_STAGE:
                state->percentage = NAN;
                return;
        }
    }

private:
    const MapReduceClient &client;
    const InputVec &inputVec;
    OutputVec &outputVec;
    IntermediateMap shuffleMap;
    std::vector<WorkerThread> workerThreads;
    std::vector<K2 *> outputKeys;
    WorkerThread shuffleThread;
    pthread_mutex_t outputVecMutex;
    pthread_mutex_t waitMutex;
    pthread_mutex_t shuffleWaitMutex;
    pthread_cond_t shufflePhase;
    pthread_cond_t jobDone;
    std::atomic<int> sharedIndex;
    std::atomic<int> numOfMappingFinished;
    std::atomic<int> stage;
    std::atomic<int> mappedKeys;
    std::atomic<int> shuffledKeys;
    std::atomic<int> reducedKeys;
};


JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                            OutputVec &outputVec, int multiThreadLevel)
{
    return new Job(client, inputVec, outputVec, multiThreadLevel);
}

void emit2(K2 *key, V2 *value, void *context)
{
    ((Job::WorkerThread *) context)->emit2(key, value);
}

void emit3(K3 *key, V3 *value, void *context)
{
    ((Job::WorkerThread *) context)->emit3(key, value);
}

void waitForJob(JobHandle job)
{
    ((Job *) job)->waitForJob();
}

void getJobState(JobHandle job, JobState *state)
{
    ((Job *) job)->getJobState(state);
}

void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    delete (Job *) job;
}
