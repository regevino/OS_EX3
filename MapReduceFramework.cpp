//
// Created by Dan on 5/20/2020.
//

#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>

class Job
{
public:
    Job(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
        int multiThreadLevel) : client(client), inputVec(inputVec), outputVec(outputVec)
    {

    }

    void threadWork()
    {
        //TODO atomic
        client.map(inputVec[0].first, inputVec[0].second, this);  //TODO change index
        while (1)
        {

        }
//        pthread_cond_wait();
//        client.reduce();
    }

private:
    const MapReduceClient &client;
    const InputVec &inputVec;
    OutputVec &outputVec;
//    int multiThreadLevel;
};

JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                            OutputVec &outputVec, int multiThreadLevel)
{

}

void emit2(K2 *key, V2 *value, void *context)
{

}

void emit3(K3 *key, V3 *value, void *context)
{

}

void waitForJob(JobHandle job)
{

}

void getJobState(JobHandle job, JobState *state)
{

}

void closeJobHandle(JobHandle job)
{

}
