//
// Created by Dan on 5/20/2020.
//

#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include <iostream>

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
                : job(job), isShuffleThread(isShuffle), mutex(PTHREAD_MUTEX_INITIALIZER)
        {
        }
        void threadWork()
        {
            if (!isShuffleThread)
            {
                int oldIndex = (job.sharedIndex)++;
                while (oldIndex < job.inputVec.size())
                {
                    job.client.map(job.inputVec[oldIndex].first, job.inputVec[oldIndex].second, this);
                    oldIndex = (job.sharedIndex)++;
                }
                pthread_mutex_lock(&mutex);
                job.numOfMappingFinished++;
                std::cerr << "FINISHED MAPPING\n";
                pthread_cond_wait(&(job.shufflePhase), &mutex);
                pthread_mutex_unlock(&mutex);
            }
            else
            {
                while (job.numOfMappingFinished < job.workerThreads.size())
                {
                    for (auto &workerThread: job.workerThreads)
                    {
                        pthread_mutex_lock(&workerThread.mutex);
                        if (!workerThread.outputVector.empty())
                        {
                            auto output = workerThread.outputVector.back();
                            workerThread.outputVector.pop_back();
                            pthread_mutex_unlock(&workerThread.mutex);
                            job.shuffleMap[output.first].push_back(output.second);
                        }
                        else
                        {
                            pthread_mutex_unlock(&workerThread.mutex);
                        }
                    }
                }
                std::cerr << "FINISHED SHUFFLE\n";
                std::transform(job.shuffleMap.begin(), job.shuffleMap.end(),
                        std::back_inserter(job.outputKeys),
                        [](const std::pair<K2* const,std::vector<V2*>>&pair){ return pair.first;});
                job.sharedIndex = 0;
                job.numOfMappingFinished = 0;
                pthread_cond_broadcast(&(job.shufflePhase));
            }

            std::cerr << "STARTED REDUCE\n";
            int oldIndex = (job.sharedIndex)++;
            while (oldIndex < job.outputKeys.size())
            {
                auto key = job.outputKeys[oldIndex];
                job.client.reduce(key, job.shuffleMap[key], this);
                oldIndex = (job.sharedIndex)++;
            }

            int numFinished = job.numOfMappingFinished++;

            std::cerr << "FINISHED REDUCE WITH INDEX " << job.sharedIndex <<"\n";
            if (numFinished == job.workerThreads.size())
            {
                pthread_cond_broadcast(&job.jobDone);
            }
        }
        void emit2(K2 *key, V2 *value)
        {
            pthread_mutex_lock(&mutex);
            outputVector.emplace_back(key, value);
            pthread_mutex_unlock(&mutex);
        }
        void emit3(K3 *key, V3 *value)
        {
            pthread_mutex_lock(&job.outputVecMutex);
            job.outputVec.emplace_back(key, value);
            pthread_mutex_unlock(&job.outputVecMutex);
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
                                workerThreads(multiThreadLevel - 1, WorkerThread(*this)),
                                shufflePhase(PTHREAD_COND_INITIALIZER), shuffleThread(*this, true),
                                outputVecMutex(PTHREAD_MUTEX_INITIALIZER),
                                waitMutex(PTHREAD_MUTEX_INITIALIZER),
                                jobDone(PTHREAD_COND_INITIALIZER)
    {
        for (auto &thread: workerThreads)
        {
            pthread_create(&thread.thread, nullptr, WorkerThread::runThread, &thread);
        }
        pthread_create(&shuffleThread.thread, nullptr, WorkerThread::runThread, &shuffleThread);
    }

    void waitForJob()
    {
        pthread_mutex_lock(&waitMutex);
        pthread_cond_wait(&jobDone, &waitMutex);
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

}

void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    delete (Job *) job;
}
