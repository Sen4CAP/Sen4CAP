#ifndef PERSISTENCEITFMODULE_H
#define PERSISTENCEITFMODULE_H

#include "persistencemanager.hpp"
#include "processorexecutioninfos.h"

/**
 * @brief The PersistenceItfModule class
 * \note
 * This class represents the interface with the persistence manager
 * application. 
 *
 */

#define SRV_IP_ADDR                 "executor.listen-ip"
#define SRV_PORT_NO                 "executor.listen-port"
#define EXECUTOR_SACCT_RETRY_CNT    "executor.sacct-max-retries"
#define PROCESSOR_WRAPPER_PATH      "executor.wrapper-path"
#define WRP_SEND_RETRIES_NO         "executor.wrp-send-retries-no"
#define WRP_TIMEOUT_BETWEEN_RETRIES "executor.wrp-timeout-between-retries"
#define WRP_EXECUTES_LOCAL          "executor.wrp-executes-local"

#define INTER_PROC_COM_TYPE         "general.inter-proc-com-type"
#define ORCHESTRATOR_TMP_PATH       "general.scratch-path"

class PersistenceItfModule : public QObject
{
    Q_OBJECT

public:
    ~PersistenceItfModule();

    static PersistenceItfModule *GetInstance();

    void RequestConfiguration();

    void MarkStepPendingStart(int taskId, QString &name);
    void MarkStepStarted(int taskId, QString &name);
    bool MarkStepFinished(int taskId, QString &name, ProcessorExecutionInfos &statistics);
    void MarkStepFailed(int taskId, QString &name, ProcessorExecutionInfos &statistics);

    // Slurm configuration
    QString GetExecutorQos(int processorId);
    QString GetExecutorPartition(int processorId);

    PersistenceManagerDBProvider * GetDBProvider();

signals:
    void OnConfigurationReceived();

private:
    PersistenceItfModule();
    PersistenceManagerDBProvider clientInterface;

    bool GetValueForKey(const ConfigurationParameterValueList &configuration,
                        const QString &key, QString &value);

    void SaveMainConfigKeys(const ConfigurationParameterValueList &configuration);
    long ParseTimeStr(const QString &strTime);
    ExecutionStatistics InitStatistics(const ProcessorExecutionInfos &statistics);
};

#endif // PERSISTENCEITFMODULE_H
