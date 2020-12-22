#include "dbusexecutorproxy.hpp"
#include "make_unique.hpp"
#include "dbus_future_utils.hpp"

DBusExecutorProxy::DBusExecutorProxy()
    : executorClient(OrgEsaSen2agriProcessorsExecutorInterface::staticInterfaceName(),
                QStringLiteral("/org/esa/sen2agri/processorsExecutor"),
                QDBusConnection::systemBus())
{
}

DBusExecutorProxy::~DBusExecutorProxy()
{
}

void DBusExecutorProxy::SubmitJob(int)
{
    // Nothing to do ... operation not supported, only steps are supported via dbus
}

void DBusExecutorProxy::CancelJob(int)
{
    // Nothing to do ... operation not supported, only steps are supported via dbus
}

void DBusExecutorProxy::PauseJob(int)
{
    // Nothing to do ... operation not supported, only steps are supported via dbus
}

void DBusExecutorProxy::ResumeJob(int)
{
    // Nothing to do ... operation not supported, only steps are supported via dbus
}

void DBusExecutorProxy::SubmitSteps(NewExecutorStepList steps)
{
    WaitForResponseAndThrow(executorClient.SubmitSteps(steps));
}

void DBusExecutorProxy::CancelTasks(TaskIdList tasks)
{
    WaitForResponseAndThrow(executorClient.CancelTasks(tasks));
}
