#include <stdio.h>
#include <QDateTime>
#include <QString>

#include <string>
using namespace std;

#include "resourcemanageritf_slurm.h"
#include "commandinvoker.h"

#include "slurmsacctresultparser.h"
#include "processorwrapperfactory.h"
#include "logger.hpp"
#include "configurationmgr.h"
#include "persistenceitfmodule.h"

#include "requestparamssubmitsteps.h"
#include "requestparamscanceltasks.h"

//#define TEST
#ifdef TEST
QString SRUN_CMD("./SlurmSrunSimulator --job-name");
QString SACCT_CMD("./SlurmSrunSimulator "
                  "--format=JobID,JobName,NodeList,AveCPU,UserCPU,SystemCPU,ExitCode,AveVMSize,"
                  "MaxRSS,MaxVMSize,MaxDiskRead,MaxDiskWrite");
QString SCANCEL_CMD("scancel --name=");
#else

static QString SRUN_CMD("srun");
static QString SRUN_JOB_NAME_PARAM("--job-name");
static QString SRUN_QOS_PARAM("--qos");
static QString SRUN_PARTITION_PARAM("--partition");

static QString SACCT_CMD("sacct");

static QString SACCT_CMD_ARG1("--parsable2");
static QString
    SACCT_CMD_ARG2("--format=JobID,JobName,NodeList,AveCPU,UserCPU,SystemCPU,ExitCode,AveVMSize,"
                   "MaxRSS,MaxVMSize,MaxDiskRead,MaxDiskWrite");

static QString SCANCEL_CMD("scancel");
static QString SCANCEL_CMD_ARGS("--name");

static QString SBATCH_CMD("sbatch");
#endif

#define SACCT_RETRY_CNT                 10
#define SACCT_RETRY_TIMEOUT_IN_MSEC     1000

ResourceManagerItf_SLURM::ResourceManagerItf_SLURM()
{
}

ResourceManagerItf_SLURM::~ResourceManagerItf_SLURM()
{
}

bool ResourceManagerItf_SLURM::HandleJobOperations(RequestParamsJobOps *)
{
    // TODO: Not supported for now
    return false;
}

/**
 * @brief RessourceManagerItf_SLURM::StartProcessor
 */
bool ResourceManagerItf_SLURM::HandleStartSteps(RequestParamsSubmitSteps *pReqParams)
{
    // Get the path to the processor wrapper and pass to it the name of
    // the processor to be executed
    QString strProcWrpExecStr;
    QString str;
    QString strIpVal;
    QString strPortVal;
    QString strWrpSendRetriesNoVal;
    QString strWrpTimeoutBetweenRetriesVal;
    QString strWrpExecutesLocalVal;
    QString strJobName;
    QList<ExecutionStep>::const_iterator stepIt;
    int i = 0;

    if (!ConfigurationMgr::GetInstance()->GetValue(PROCESSOR_WRAPPER_PATH, strProcWrpExecStr)) {
        Logger::error(QString(
            "The path for the processor wrapper was not found. Please check configuration"));
        return false;
    }
    ConfigurationMgr::GetInstance()->GetValue(SRV_IP_ADDR, strIpVal);
    ConfigurationMgr::GetInstance()->GetValue(SRV_PORT_NO, strPortVal);
    ConfigurationMgr::GetInstance()->GetValue(WRP_SEND_RETRIES_NO, strWrpSendRetriesNoVal, "60");
    ConfigurationMgr::GetInstance()->GetValue(WRP_TIMEOUT_BETWEEN_RETRIES, strWrpTimeoutBetweenRetriesVal, "1000");
    ConfigurationMgr::GetInstance()->GetValue(WRP_EXECUTES_LOCAL, strWrpExecutesLocalVal, "1");

    QList<ExecutionStep> &execSteps = pReqParams->GetExecutionSteps();
    for (stepIt = execSteps.begin(); stepIt != execSteps.end(); stepIt++) {
        ExecutionStep executionStep = (*stepIt);
        strJobName = BuildStepExecutionName(executionStep.GetTaskId(), executionStep.GetStepName(), i);
        i++;
        // The following parameters are sent to the processor wrapper:
        //      PROC_PATH=<path to the processor>
        //      PROC_PARAMS=<parameters for the processor>
        //      JOB_NAME=<name of the slurm job> - this will be optional
        //      SRV_IP_ADDR=<IP address of this server> - optional
        //      SRV_PORT_NO=<port of this server> - optional

        QStringList listParams;
        QString strParam;
        QString strQos = PersistenceItfModule::GetInstance()->GetExecutorQos(executionStep.GetProcessorId());
        QString strPartition = PersistenceItfModule::GetInstance()->GetExecutorPartition(executionStep.GetProcessorId());
        if(!strQos.isEmpty()) {
            listParams.push_back(SRUN_QOS_PARAM);
            listParams.push_back(strQos);

        }
        if(!strPartition.isEmpty()) {
            listParams.push_back(SRUN_PARTITION_PARAM);
            listParams.push_back(strPartition);
        }
        listParams.push_back(SRUN_JOB_NAME_PARAM);
        listParams.push_back(strJobName);
        listParams.push_back(strProcWrpExecStr);

        strParam = QString("%1=%2").arg("SRV_IP_ADDR", strIpVal);
        listParams.append(strParam);
        strParam = QString("%1=%2").arg("SRV_PORT_NO", strPortVal);
        listParams.push_back(strParam);

        strParam = QString("%1=%2").arg("WRP_SEND_RETRIES_NO", strWrpSendRetriesNoVal);
        listParams.push_back(strParam);
        strParam = QString("%1=%2").arg("WRP_TIMEOUT_BETWEEN_RETRIES", strWrpTimeoutBetweenRetriesVal);
        listParams.push_back(strParam);
        strParam = QString("%1=%2").arg("WRP_EXECUTES_LOCAL", strWrpExecutesLocalVal);
        listParams.push_back(strParam);

        strParam = QString("%1=%2").arg("JOB_NAME", strJobName);
        listParams.push_back(strParam);

        // build the processor path to be execute along with its parameters
        // by adding also to the parameters list the processor name with its key (S2_PROC_NAME)
        strParam = QString("%1=%2").arg("PROC_PATH", executionStep.GetProcessorPath());
        listParams.push_back(strParam);

        if (!executionStep.GetArgumentsList().isEmpty()) {
            listParams.push_back("PROC_PARAMS");
            listParams.append(executionStep.GetArgumentsList());
        }

        QString paramsStr = BuildParamsString(listParams);
        // Build the srun command to be executed in SLURM - no need to wait
        // QString strSrunCmd = QString("%1 %2 %3").arg(SRUN_CMD, strJobName, paramsStr);

        Logger::debug(QString("HandleStartProcessor: Executing command %1 with params %2")
                          .arg(SRUN_CMD, paramsStr));

        QTemporaryFile tempFile;
        if (tempFile.open()) {
            //tempFile.setAutoRemove(false);
            // TODO: The simplest way would be to send an inline shell script but I don't know why it doesn't work
            //QString cmd = QString(" <<EOF\n#!/bin/sh\nsrun %1\nEOF").arg(paramsStr);
            QString cmd = QString("#!/bin/sh\nsrun %1").arg(paramsStr);
           tempFile.write(cmd.toStdString().c_str(), cmd.length());
           tempFile.flush();
           if(!tempFile.setPermissions(QFile::ReadOwner|
                                       QFile::WriteOwner|
                                       QFile::ExeOwner|
                                       QFile::ReadGroup|
                                       QFile::ExeGroup|
                                       QFile::ReadOther|QFile::ExeOther)) {
               Logger::error(QString("Unable to execute SLURM sbatch command for the processor %1. "
                                     "The execution permissions cannot be set for the script!")
                                 .arg(executionStep.GetProcessorPath()));
               return false;
           }
        } else {
            Logger::error(QString("Unable to execute SLURM sbatch command for the processor %1. "
                                  "The script cannot be created!")
                              .arg(executionStep.GetProcessorPath()));
            return false;
        }
        QStringList sbatchParams;
        // Set the job nane of the batch the same as the inner job runs
        // this is useful to have the task name in slurm something we can use instead of the
        // default usage of the name of the batch script
        sbatchParams.push_back(SRUN_JOB_NAME_PARAM);
        sbatchParams.push_back(strJobName);

        //sbatchParams.push_back(QString(" <<EOF\n#!/bin/sh\nsrun %1\nEOF").arg(paramsStr));
        if(!strQos.isEmpty()) {
            sbatchParams.push_back(SRUN_QOS_PARAM);
            sbatchParams.push_back(strQos);

        }
        if(!strPartition.isEmpty()) {
            sbatchParams.push_back(SRUN_PARTITION_PARAM);
            sbatchParams.push_back(strPartition);
        }

        sbatchParams.push_back(tempFile.fileName());
        QString batchParamsStr = BuildParamsString(sbatchParams);
        Logger::debug(QString("HandleStartProcessor: Executing command %1 with params %2")
                          .arg(SBATCH_CMD, batchParamsStr));

        CommandInvoker cmdInvoker;
        if (!cmdInvoker.InvokeCommand(SBATCH_CMD, sbatchParams, false)) {
            Logger::error(QString("Unable to execute SLURM sbatch command for the processor %1. The error was: \"%2\"")
                              .arg(executionStep.GetProcessorPath(), cmdInvoker.GetExecutionLog()));
            return false;
        }
        Logger::debug(QString("HandleStartProcessor: Sbatch command returned: \"%1\"")
                          .arg(cmdInvoker.GetExecutionLog()));

        // send the name of the job and the time to the persistence manager
        PersistenceItfModule::GetInstance()->MarkStepPendingStart(executionStep.GetTaskId(),
                                                                  executionStep.GetStepName());
    }

    return true;
}

/**
 * @brief RessourceManagerItf_SLURM::StopProcessor
 */
void ResourceManagerItf_SLURM::HandleStopTasks(RequestParamsCancelTasks *pReqParams)
{
    QList<ProcessorExecutionInfos> procExecResults;
    if (GetSacctResults(SACCT_CMD, procExecResults)) {
        QList<int>::const_iterator idIt;
        QList<ProcessorExecutionInfos>::const_iterator procInfosIt;
        QString stepId;
        QList<int> listIds = pReqParams->GetTaskIdsToCancel();
        for (idIt = listIds.begin(); idIt != listIds.end(); idIt++) {
            stepId = QString("TSKID_%1_STEPNAME_").arg(*idIt);
            for (procInfosIt = procExecResults.begin(); procInfosIt != procExecResults.end();
                 procInfosIt++) {
                if ((*procInfosIt).strJobName.startsWith(stepId)) {
                    CommandInvoker cmdScancelInvoker;

                    // run scancel command and wait for it to return
                    QStringList args;
                    args << SCANCEL_CMD_ARGS << (*procInfosIt).strJobName;

                    Logger::debug(QString("HandleStopProcessor: Executing command %1 %2")
                                      .arg(SCANCEL_CMD)
                                      .arg(args.join(' ')));

                    if (!cmdScancelInvoker.InvokeCommand(SCANCEL_CMD, args, false)) {
                        // Log the execution trace here
                        Logger::error("Error executing SCANCEL command");
                    }

                    // TODO: See if this is really required or can be removed
                    //(*procInfosIt).strJobStatus = ProcessorExecutionInfos::g_strCanceled;
                    // Send the information about this job to the Persistence Manager
                    // PersistenceItfModule::GetInstance()->SendProcessorExecInfos((*procInfosIt));
                }
            }
        }
    }
}

void ResourceManagerItf_SLURM::FillEndStepAddExecInfos(const QString &strJobName, ProcessorExecutionInfos &info)
{
    CommandInvoker cmdInvoker;
    QStringList args;
    args << SACCT_CMD_ARG1 << SACCT_CMD_ARG2;

    Logger::debug(QString("HandleProcessorEndedMsg: Executing command %1 %2")
                      .arg(SACCT_CMD)
                      .arg(args.join(' ')));

    int retryCnt = 0;
    while(retryCnt < SACCT_RETRY_CNT) {
        if (cmdInvoker.InvokeCommand(SACCT_CMD, args, false)) {
            const QString &strLog = cmdInvoker.GetExecutionLog();
            SlurmSacctResultParser slurmSacctParser;
            QList<ProcessorExecutionInfos> procExecResults;
            if (slurmSacctParser.ParseResults(strLog, procExecResults, strJobName) > 0) {
                info = procExecResults.at(0);
                break;
            } else {
                Logger::error(
                    QString("Unable to parse SACCT results for job name %1. Retrying ...").arg(strJobName));
                msleep(SACCT_RETRY_TIMEOUT_IN_MSEC);
            }
        } else {
            Logger::error(
                QString("Error executing SACCT for job name %1").arg(strJobName));
            break;
        }
        retryCnt++;
    }

}

bool ResourceManagerItf_SLURM::GetSacctResults(QString &sacctCmd,
                                          QList<ProcessorExecutionInfos> &procExecResults)
{
    CommandInvoker cmdInvoker;

    QStringList args;
    args << SACCT_CMD_ARG1 << SACCT_CMD_ARG2;

    if (cmdInvoker.InvokeCommand(sacctCmd, args, false)) {
        const QString &strLog = cmdInvoker.GetExecutionLog();
        SlurmSacctResultParser slurmSacctParser;
        if (slurmSacctParser.ParseResults(strLog, procExecResults) > 0) {
            return true;
        } else {
            Logger::error(QString("Unable to parse SACCT results"));
        }
    }
    return false;
}

