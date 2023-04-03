#include <iostream>
using namespace std;
#include <functional>

#include <QDBusConnectionInterface>
#include <QDBusMessage>
#include <QThreadPool>
#include <QSettings>
#include "persistencemanager.h"

PersistenceManager::PersistenceManager(QObject *parent)
    : QObject(parent)
{
}

ConfigurationSet PersistenceManager::GetConfigurationSet()
{
    return {};
}

ConfigurationParameterValueList PersistenceManager::GetConfigurationParameters(QString prefix)
{
    QSettings settings ("./persistence.ini", QSettings::IniFormat);
    QString strIp = settings.value("SRV_IP", "127.0.0.1").toString();
    QString strPort = settings.value("PORT_NO", "7777").toString();
    QString strWrapperPath = settings.value("WRAPPER_PATH", "./sen2agri-processor-wrapper").toString();

    cout << "----------------------------------------------------\n";
    cout << "PersistenceManagerSimulator: GetConfigurationParameters called!\n";
    cout << "PREFIX : " << prefix.toStdString().c_str() << '\n';
    cout << "----------------------------------------------------\n";
    ConfigurationParameterValueList retList(
        {ConfigurationParameterValue("executor.listen-ip", 0, strIp),
         ConfigurationParameterValue("executor.listen-port", 0, strPort),
         ConfigurationParameterValue("executor.wrapper-path", 0, strWrapperPath)
/*         ConfigurationParameterValue("executor.processor.l2a.name", 0, m_pSettings->value("PROCESSOR_1_NAME", "CROP_TYPE").toString()),
         ConfigurationParameterValue("executor.processor.l2a.path", 0, m_pSettings->value("PROCESSOR_1_PATH", "./DummyProcessor").toString()),
         ConfigurationParameterValue("executor.processor.l3a.name", 0, "ATM_CORR"),
         ConfigurationParameterValue("executor.processor.l3a.path", 0, "atm_corrections.exe"),
         ConfigurationParameterValue("executor.processor.l3b.name", 0, "y"),
         ConfigurationParameterValue("executor.processor.l3b.path", 0, "y"),
         ConfigurationParameterValue("executor.processor.l4a.name", 0, "y"),
         ConfigurationParameterValue("executor.processor.l4a.path", 0, "y"),
         ConfigurationParameterValue("executor.processor.l4b.name", 0, "y"),
         ConfigurationParameterValue("executor.processor.l4b.path", 0, "y")*/});

    return retList;
}

JobConfigurationParameterValueList PersistenceManager::GetJobConfigurationParameters(int jobId,
                                                                                     QString prefix)
{
    return {};
}

KeyedMessageList
PersistenceManager::UpdateConfigurationParameters(ConfigurationUpdateActionList parameters)
{
    return {};
}

KeyedMessageList
PersistenceManager::UpdateJobConfigurationParameters(int jobId,
                                                     JobConfigurationUpdateActionList parameters)
{
    return {};
}

ProductToArchiveList PersistenceManager::GetProductsToArchive()
{
    return {};
}

void PersistenceManager::MarkProductsArchived(ArchivedProductList products)
{
}

int PersistenceManager::SubmitJob(NewJob job)
{
    return {};
}

int PersistenceManager::SubmitTask(NewTask task)
{
    return {};
}

void PersistenceManager::SubmitSteps(int taskId, NewStepList steps)
{
}

void PersistenceManager::MarkStepPendingStart(int taskId, QString name)
{
    cout << "----------------------------------------------------\n";
    cout << "PersistenceManager: MarkStepPendingStart called with task id " << taskId << '\n';
    cout << "STEP NAME : " << name.toStdString().c_str() << '\n';
    cout << "----------------------------------------------------\n";
}

void PersistenceManager::MarkStepStarted(int taskId, QString name)
{
    cout << "----------------------------------------------------\n";
    cout << "PersistenceManager: MarkStepStarted called with task id " << taskId << '\n';
    cout << "STEP NAME : " << name.toStdString().c_str() << '\n';
    cout << "----------------------------------------------------\n";
}

bool PersistenceManager::MarkStepFinished(int taskId, QString name, ExecutionStatistics statistics)
{
    cout << "----------------------------------------------------\n";
    cout << "PersistenceManager: MarkStepFinished called with task id " << taskId << '\n';
    cout << "STEP NAME : " << name.toStdString().c_str() << '\n';
    cout << "diskReadBytes: " << statistics.diskReadBytes << '\n';
    cout << "diskWriteBytes: " << statistics.diskWriteBytes << '\n';
    cout << "durationMs: " << statistics.durationMs << '\n';
    cout << "exitCode: " << statistics.exitCode << '\n';
    cout << "maxRssKb: " << statistics.maxRssKb << '\n';
    cout << "maxVmSizeKb: " << statistics.maxVmSizeKb << '\n';
    cout << "node: " << statistics.node.toStdString().c_str() << '\n';
    cout << "systemCpuMs: " << statistics.systemCpuMs << '\n';
    cout << "userCpuMs: " << statistics.userCpuMs << '\n';
    cout << "----------------------------------------------------\n";

    return {};
}

void PersistenceManager::MarkJobPaused(int jobId)
{
}

void PersistenceManager::MarkJobResumed(int jobId)
{
}

void PersistenceManager::MarkJobCancelled(int jobId)
{
}

void PersistenceManager::MarkJobFinished(int jobId)
{
}

void PersistenceManager::MarkJobFailed(int jobId)
{
}

void PersistenceManager::MarkJobNeedsInput(int jobId)
{
}

TaskIdList PersistenceManager::GetJobTasksByStatus(int jobId, ExecutionStatusList statusList)
{
    return {};
}

JobStepToRunList PersistenceManager::GetTaskStepsForStart(int taskId)
{
    return {};
}

JobStepToRunList PersistenceManager::GetJobStepsForResume(int jobId)
{
    return {};
}

void PersistenceManager::InsertTaskFinishedEvent(TaskFinishedEvent event)
{
}

void PersistenceManager::InsertProductAvailableEvent(ProductAvailableEvent event)
{
}

void PersistenceManager::InsertJobCancelledEvent(JobCancelledEvent event)
{
}

void PersistenceManager::InsertJobPausedEvent(JobPausedEvent event)
{
}

void PersistenceManager::InsertJobResumedEvent(JobResumedEvent event)
{
}

void PersistenceManager::InsertJobSubmittedEvent(JobSubmittedEvent event)
{
}

UnprocessedEventList PersistenceManager::GetNewEvents()
{
    return {};
}

void PersistenceManager::MarkEventProcessingStarted(int eventId)
{
}

void PersistenceManager::MarkEventProcessingComplete(int eventId)
{
}

void PersistenceManager::InsertNodeStatistics(NodeStatistics statistics)
{
}

QString PersistenceManager::GetDashboardData(QDate since)
{
    return {};
}

