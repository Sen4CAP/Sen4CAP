#include <stdexcept>
#include <QCoreApplication>
#include <QDBusError>

#include "logger.hpp"
#include "orchestratorrequestshandler.h"
#include "configurationmgr.h"
#include "execinfosprotsrvfactory.h"
#include "persistenceitfmodule.h"

#include "adaptor/dbusexecutoradaptor.h"
#include "adaptor/httpexecutoradaptor.h"

#include "configuration.hpp"

#include "resourcemanager/resourcemanagerfactory.h"
#include "resourcemanager/resourcemanageritf_tao.h"

int main(int argc, char *argv[])
{
    try {
        QString str;
        QCoreApplication a(argc, argv);

        qDebug() << "Current execution path: " << QDir::currentPath();

        registerMetaTypes();

        // get the configuration from the persistence manager
        PersistenceItfModule::GetInstance()->RequestConfiguration();

        // Create the resource manager
        AbstractResourceManagerItf *pResMng = ResourceManagerFactory::GetResourceManager();

        // Initialize the server for receiving the messages from the executing Processor Wrappers
        QString strIpVal;
        QString strPortVal;
        AbstractExecInfosProtSrv *pExecInfosSrv =
            ExecInfosProtSrvFactory::GetInstance()->CreateExecInfosProtSrv(
                ExecInfosProtSrvFactory::SIMPLE_TCP);
        ConfigurationMgr::GetInstance()->GetValue(SRV_IP_ADDR, strIpVal);
        ConfigurationMgr::GetInstance()->GetValue(SRV_PORT_NO, strPortVal);
        pExecInfosSrv->SetProcMsgListener(pResMng);
        if (!pExecInfosSrv->StartCommunication(strIpVal, strPortVal.toInt())) {
            QString str = QString("Unable start communication for IP %1 and port %2. The application cannot start!")
                    .arg(strIpVal).arg(strPortVal);
            throw std::runtime_error(str.toStdString());
        }

        // start the ressource manager
        pResMng->Start();

        OrchestratorRequestsHandler orchestratorReqHandler;

        str = QString(INTER_PROC_COM_TYPE);
        QString interProcCommType;
        ConfigurationMgr::GetInstance()->GetValue(str, interProcCommType);
        if (interProcCommType == "http") {
            // Initialize the HTTP for Orchestrator requests handling
            new HttpExecutorAdaptor(*PersistenceItfModule::GetInstance()->GetDBProvider(), &orchestratorReqHandler);
        } else {
            // Initialize the DBus for Orchestrator requests handling
            new DBusExecutorAdaptor(&orchestratorReqHandler);
        }

        return a.exec();
    } catch (const std::exception &e) {
        Logger::fatal(e.what());

        return EXIT_FAILURE;
    }
}
