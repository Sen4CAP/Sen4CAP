#pragma once

#include "persistencemanager.hpp"
#include "tasktosubmit.hpp"
#include <qobject.h>
#include <qstringlist.h>

class StepExecutionDecorator  : public QObject
{
    Q_OBJECT

public :
    StepExecutionDecorator();
    ~StepExecutionDecorator();

    static StepExecutionDecorator *GetInstance();
    NewStep CreateTaskStep(const QString &procName, TaskToSubmit &task, const QString &stepName, const QStringList &stepArgs);
    QStringList GetDockerMounts(const QString &procName, const QString &taskName);

private:
    QStringList UpdateCommandForDocker(const QString &taskName, const QStringList &arguments,
                                       const QString& dockerImage, const QStringList &dockerMounts);
    // QStringList UpdateSimpleCommandArgs(const QString &taskName, const QStringList &arguments);
    QString GetArchiverRootPath();
    bool IsDockerEnabledForStep(const QString &procName, const QString &taskName);
    QStringList EnsureUniqueDockerMounts(const QString &mounts);
    bool HasMount(const QStringList &mounts, const QString &mount);
    QString GetOrchestratorStepConfigValue(const QString &procName, const QString &taskName, const QString &key, QString &foundRoot);
    QString GetParamValue(const ConfigurationParameterValueList &parameters, const QString &key, const QString &defVal);
    QString GetScratchPathRoot(const QString &procName);
    std::map<QString, QString> GetModulePathMap();

private:
    PersistenceManagerDBProvider dbProvider;

    ConfigurationParameterValueList m_orchestratorConfig;
    ConfigurationParameterValueList m_ScratchPathConfig;
    ConfigurationParameterValueList m_modulePathsConfig;
    QString m_archiverPath;
    QString m_configPath;

};
