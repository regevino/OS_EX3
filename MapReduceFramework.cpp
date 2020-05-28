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

class Job
{
public:
    class WorkerThread
    {
    private:
        std::vector<std::pair<K2*, V2*>> outputVector;
        Job &job;
        pthread_mutex_t mutex;
    public:
        bool isShuffleThread;
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

        void threadWork()
        {
            if (!isShuffleThread)
            {
                int oldIndex = (job.sharedIndex)++;
                while (oldIndex < (int)job.inputVec.size())
                {
                    job.client.map(job.inputVec[oldIndex].first, job.inputVec[oldIndex].second, this);
                    oldIndex = (job.sharedIndex)++;
                }
                int previousNum = job.numOfMappingFinished++;
                if (previousNum == (int)job.workerThreads.size() - 1)
                {
                    job.stage = stage_t::SHUFFLE_STAGE;
                }
                if(pthread_mutex_lock(&job.shuffleWaitMutex))
                {
                    std::cerr << SYS_ERR << MUTEX_LOCK_ERR;
                    exit(EXIT_FAILURE);
                }
                if(pthread_cond_wait(&(job.shufflePhase), &job.shuffleWaitMutex))
                {
                    std::cerr << SYS_ERR << COND_WAIT_ERR;
                    exit(EXIT_FAILURE);
                }
                if(pthread_mutex_unlock(&job.shuffleWaitMutex))
                {
                    std::cerr << SYS_ERR << MUTEX_UNLOCK_ERR;
                    exit(EXIT_FAILURE);
                }
            }
            else
            {
                bool moreToShuffle = true;
                while (moreToShuffle || job.numOfMappingFinished < (int)job.workerThreads.size())
                {
                    moreToShuffle = false;
                    for (auto &workerThread: job.workerThreads)
                    {
                        if(pthread_mutex_lock(&workerThread.mutex))
                        {
                            std::cerr << SYS_ERR << MUTEX_LOCK_ERR;
                            exit(EXIT_FAILURE);
                        }
                        if (!workerThread.outputVector.empty())
                        {
                            auto output = workerThread.outputVector.back();
                            workerThread.outputVector.pop_back();
                            if (!workerThread.outputVector.empty())
                            {
                                moreToShuffle = true;
                            }
                            if(pthread_mutex_unlock(&workerThread.mutex))
                            {
                                std::cerr << SYS_ERR << MUTEX_UNLOCK_ERR;
                                exit(EXIT_FAILURE);
                            }
                            job.shuffleMap[output.first].push_back(output.second);
                            ++job.shuffledKeys;
                        }
                        else
                        {
                            if(pthread_mutex_unlock(&workerThread.mutex))
                            {
                                std::cerr << SYS_ERR << MUTEX_UNLOCK_ERR;
                                exit(EXIT_FAILURE);
                            }
                        }
                    }
                }
                std::transform(job.shuffleMap.begin(), job.shuffleMap.end(),
                        std::back_inserter(job.outputKeys),
                        [](const std::pair<K2* const,std::vector<V2*>>&pair){ return pair.first;});
                job.numOfMappingFinished = 0;
                job.stage = stage_t::REDUCE_STAGE;
                job.sharedIndex = 0;
                if (pthread_cond_broadcast(&(job.shufflePhase)))
                {
                    std::cerr << SYS_ERR << COND_BROAD_ERR;
                    exit(EXIT_FAILURE);
                }
            }

            int oldIndex = (job.sharedIndex)++;
            while (oldIndex < (int)job.outputKeys.size())
            {
                auto key = job.outputKeys[oldIndex];
                assert(job.shuffleMap.count(key) > 0);
                job.client.reduce(key, job.shuffleMap[key], this);
                oldIndex = (job.sharedIndex)++;
            }

            if (this->isShuffleThread)
            {
                for (auto &th: job.workerThreads)
                {
                    if (pthread_join(th.thread, nullptr))
                    {
                        std::cerr << SYS_ERR << JOIN_ERR;
                        exit(EXIT_FAILURE);
                    }
                }
//                for (auto &e: job.outputVec)
//                {
//                    std::cerr << "key: " << e.first << ", value: " << e.second << "\n";
//                }
                if(pthread_mutex_lock(&job.waitMutex))
                {
                    std::cerr << SYS_ERR << MUTEX_LOCK_ERR;
                    exit(EXIT_FAILURE);
                }
                if (pthread_cond_broadcast(&job.jobDone))
                {
                    std::cerr << SYS_ERR << COND_BROAD_ERR;
                    exit(EXIT_FAILURE);
                }
                if(pthread_mutex_unlock(&job.waitMutex))
                {
                    std::cerr << SYS_ERR << MUTEX_UNLOCK_ERR;
                    exit(EXIT_FAILURE);
                }
            }
        }
        void emit2(K2 *key, V2 *value)
        {
            if(pthread_mutex_lock(&mutex))
            {
                std::cerr << SYS_ERR << MUTEX_LOCK_ERR;
                exit(EXIT_FAILURE);
            }
            outputVector.emplace_back(key, value);
            if(pthread_mutex_unlock(&mutex))
            {
                std::cerr << SYS_ERR << MUTEX_UNLOCK_ERR;
                exit(EXIT_FAILURE);
            }
            ++job.mappedKeys;
        }
        void emit3(K3 *key, V3 *value)
        {
            if(pthread_mutex_lock(&job.outputVecMutex))
            {
                std::cerr << SYS_ERR << MUTEX_LOCK_ERR;
                exit(EXIT_FAILURE);
            }
            job.outputVec.emplace_back(key, value);
            if(pthread_mutex_unlock(&job.outputVecMutex))
            {
                std::cerr << SYS_ERR << MUTEX_UNLOCK_ERR;
                exit(EXIT_FAILURE);
            }
            ++job.reducedKeys;
        }
        static void* runThread(void* arg)
        {
            ((WorkerThread *) arg)->threadWork();
            return nullptr;
        }
    };

    Job(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
        int multiThreadLevel) : client(client), inputVec(inputVec), outputVec(outputVec),
                                sharedIndex(0), numOfMappingFinished(0),
                                shufflePhase(PTHREAD_COND_INITIALIZER),
                                workerThreads(multiThreadLevel - 1, WorkerThread(*this)),
                                shuffleThread(*this, true),
                                outputVecMutex(PTHREAD_MUTEX_INITIALIZER),
                                waitMutex(PTHREAD_MUTEX_INITIALIZER),
                                jobDone(PTHREAD_COND_INITIALIZER),
                                mappedKeys(0),
                                shuffledKeys(0),
                                reducedKeys(0),
                                shuffleWaitMutex(PTHREAD_MUTEX_INITIALIZER)
    {
        stage = stage_t::MAP_STAGE;
        for (auto &thread: workerThreads)
        {
            if(pthread_create(&thread.thread, nullptr, WorkerThread::runThread, &thread))
            {
                std::cerr << SYS_ERR << CREATE_ERR;
                exit(EXIT_FAILURE);
            }
        }
        if(pthread_create(&shuffleThread.thread, nullptr, WorkerThread::runThread, &shuffleThread))
        {
            std::cerr << SYS_ERR << CREATE_ERR;
            exit(EXIT_FAILURE);
        }
    }

    ~Job()
    {
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
//    	if (currentState.stage == stage_t::REDUCE_STAGE && currentState.percentage >= 100)
//		{
//			return;
//		}
        if(pthread_mutex_lock(&waitMutex))
        {
            std::cerr << SYS_ERR << MUTEX_LOCK_ERR;
            exit(EXIT_FAILURE);
        }
        if(pthread_cond_wait(&jobDone, &waitMutex))
        {
            std::cerr << SYS_ERR << COND_WAIT_ERR;
            exit(EXIT_FAILURE);
        }
        if(pthread_mutex_unlock(&waitMutex))
        {
            std::cerr << SYS_ERR << MUTEX_UNLOCK_ERR;
            exit(EXIT_FAILURE);
        }
    }

    void getJobState(JobState *state)
    {
        int curSharedIndex = sharedIndex;
        state->stage = (stage_t)(int)stage;
        switch (state->stage)
        {
            case stage_t::MAP_STAGE:
            	if((int)inputVec.size() <= curSharedIndex)
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
                state->percentage = 100 * (float) reducedKeys / shuffledKeys;
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
    std::atomic<int> sharedIndex;
    std::atomic<int> numOfMappingFinished;
    pthread_cond_t shufflePhase;
    std::map<K2*, std::vector<V2*>> shuffleMap;
    std::vector<WorkerThread> workerThreads;
    WorkerThread shuffleThread;
    std::vector<K2*> outputKeys;
    pthread_mutex_t outputVecMutex;
    pthread_mutex_t waitMutex;
    pthread_cond_t jobDone;
    std::atomic<int> stage;
    std::atomic<int> mappedKeys;
    std::atomic<int> shuffledKeys;
    std::atomic<int> reducedKeys;
    pthread_mutex_t shuffleWaitMutex;
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
