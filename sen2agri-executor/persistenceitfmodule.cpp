#include <iostream>
using namespace std;

#include "persistenceitfmodule.h"
#include "configurationmgr.h"
#include "settings.hpp"
#include "configuration.hpp"

PersistenceItfModule::PersistenceItfModule()
    : clientInterface(Settings::readSettings(getConfigurationFile(*QCoreApplication::instance())))
{
}

PersistenceItfModule::~PersistenceItfModule()
{
}

/*static*/
PersistenceItfModule *PersistenceItfModule::GetInstance()
{
    static PersistenceItfModule instance;
    return &instance;
}

PersistenceManagerDBProvider* PersistenceItfModule::GetDBProvider() {
    return &clientInterface;
}

void PersistenceItfModule::MarkStepPendingStart(int taskId, QString &name)
{
    clientInterface.MarkStepPendingStart(taskId, name);
}

void PersistenceItfModule::MarkStepStarted(int taskId, QString &name)
{
    clientInterface.MarkStepStarted(taskId, name);
}

bool PersistenceItfModule::MarkStepFinished(int taskId,
                                            QString &name,
                                            ProcessorExecutionInfos &statistics)
{
    // Convert ProcessorExecutionInfos to ExecutionStatistics
    const ExecutionStatistics &newStats = InitStatistics(statistics);
    return clientInterface.MarkStepFinished(taskId, name, newStats);
}

void PersistenceItfModule::MarkStepFailed(int taskId,
                                            QString &name,
                                            ProcessorExecutionInfos &statistics)
{
    // Convert ProcessorExecutionInfos to ExecutionStatistics
    const ExecutionStatistics &newStats = InitStatistics(statistics);
    clientInterface.MarkStepFailed(taskId, name, newStats);
}

void PersistenceItfModule::RequestConfiguration()
{
    // Do not catch any exception as without configuration the executor is useles
    const auto &executorKeys = clientInterface.GetConfigurationParameters("executor.");
    SaveMainConfigKeys(executorKeys);
    const auto &generalKeys = clientInterface.GetConfigurationParameters("general.");
    SaveMainConfigKeys(generalKeys);

    // SaveProcessorsConfigKeys(keys);
    emit OnConfigurationReceived();
}

QString PersistenceItfModule::GetExecutorQos(int processorId) {
    QString qos;
    const auto &strProcQosKey = QString("executor.processor.%1.slurm_qos")
            .arg(clientInterface.GetProcessorShortName(processorId));
    const auto &keys = clientInterface.GetConfigurationParameters(strProcQosKey);
    GetValueForKey(keys, strProcQosKey, qos);
    return qos;
}

QString PersistenceItfModule::GetExecutorPartition(int processorId) {
    QString partition;
    const auto &strProcPartKey = QString("executor.processor.%1.slurm_partition")
            .arg(clientInterface.GetProcessorShortName(processorId));
    const auto &keys = clientInterface.GetConfigurationParameters(strProcPartKey);
    GetValueForKey(keys, strProcPartKey, partition);
    return partition;
}

void PersistenceItfModule::SaveMainConfigKeys(const ConfigurationParameterValueList &configuration)
{
    for (const auto &p : configuration) {
        // add the original key
        ConfigurationMgr::GetInstance()->SetValue(p.key, p.value);
    }
}

bool PersistenceItfModule::GetValueForKey(
    const ConfigurationParameterValueList &configuration, const QString &key, QString &value)
{
    for (const auto &p : configuration) {
        if (p.key == key) {
            value = p.value;
            return true;
        }
    }

    return false;
}

long PersistenceItfModule::ParseTimeStr(const QString &strTime)
{
    // This function expects a string like [DD-[hh:]]mm:ss.mss
    QString strDays;
    QString strHours;
    QString strMinutes;
    QString strSeconds;
    QString strMillis;

    const QStringList &list = strTime.split(':');
    int listSize = list.size();
    const QString &firstElem = list.at(0);
    const QStringList &listDate = firstElem.split('-');
    if (listDate.size() > 1) {
        strDays = listDate.at(0);
        strHours = listDate.at(1);
        strMinutes = list.at(1);
        strSeconds = list.at(2);
    } else {
        // we have no separator for the days
        if (listSize == 3) {
            strHours = list.at(0);
            strMinutes = list.at(1);
            strSeconds = list.at(2);
        } else if (listSize == 2) {
            strMinutes = list.at(0);
            strSeconds = list.at(1);
        } else {
            // unknown format
            return -1;
        }
    }
    const QStringList &listSS = strSeconds.split('.');
    if (listSS.size() == 2) {
        strSeconds = listSS.at(0);
        strMillis = listSS.at(1);
    } else if (listSS.size() != 1) {
        // unknown format
        return -1;
    }
    long millis = (strDays.toLong() * 86400 + strHours.toLong() * 3600 + strMinutes.toLong() * 60 +
                   strSeconds.toLong()) *
                      1000 +
                  strMillis.toLong();
    return millis;
}

ExecutionStatistics PersistenceItfModule::InitStatistics(const ProcessorExecutionInfos &statistics) {
    // Convert ProcessorExecutionInfos to ExecutionStatistics
    ExecutionStatistics newStats;
    newStats.diskReadBytes = statistics.strDiskRead.toLong();
    newStats.diskWriteBytes = statistics.strDiskWrite.toLong();
    newStats.durationMs = ParseTimeStr(statistics.strCpuTime);
    newStats.exitCode = statistics.strExitCode.toInt();
    newStats.maxRssKb = statistics.strMaxRss.toInt();
    newStats.maxVmSizeKb = statistics.strMaxVmSize.toInt();
    newStats.node = statistics.strJobNode;
    newStats.systemCpuMs = ParseTimeStr(statistics.strSystemTime);
    newStats.userCpuMs = ParseTimeStr(statistics.strUserTime);
    newStats.stdOutText = statistics.strStdOutText;
    newStats.stdErrText = statistics.strStdErrText;

    return newStats;
}
