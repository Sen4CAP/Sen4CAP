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

private:
    QStringList UpdateCommandForDocker(const QString &taskName, const QStringList &arguments,
                                       const QString& dockerImage, const QString &additionalMounts);
    // QStringList UpdateSimpleCommandArgs(const QString &taskName, const QStringList &arguments);
    QString GetArchiverRootPath();
    bool IsDockerEnabledForStep(const QString &procName, const QString &taskName);
    QString GetAllDockerMounts(const QString &procName, const QString &mounts);
    QString GetDockerAdditionalMounts(const QString &procName, const QString &taskName);
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

};
