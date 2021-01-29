#include "stepexecutiondecorator.h"
#include "settings.hpp"
#include "configuration.hpp"

#include <unistd.h>
#include <sys/types.h>

#define ORCHESTRATOR_CFG_KEYS_ROOT "general.orchestrator."


StepExecutionDecorator::StepExecutionDecorator()
    : dbProvider(Settings::readSettings(getConfigurationFile(*QCoreApplication::instance())))
{
    m_orchestratorConfig = dbProvider.GetConfigurationParameters(ORCHESTRATOR_CFG_KEYS_ROOT);
    m_archiverPath = GetArchiverRootPath();
    m_ScratchPathConfig = dbProvider.GetConfigurationParameters(QStringLiteral("general.scratch-path"));
    m_modulePathsConfig = dbProvider.GetConfigurationParameters(QStringLiteral("executor.module.path."));
    if (m_ScratchPathConfig.empty()) {
        throw std::runtime_error("Please configure the \"general.scratch-path\" parameter with the "
                                 "temporary file path");
    }
    Q_ASSERT(m_ScratchPathConfig.size() >= 1);
}

StepExecutionDecorator::~StepExecutionDecorator()
{
}

/*static*/
StepExecutionDecorator *StepExecutionDecorator::GetInstance()
{
    static StepExecutionDecorator instance;
    return &instance;
}

NewStep StepExecutionDecorator::CreateTaskStep(const QString &procName, TaskToSubmit &task, const QString &stepName, const QStringList &stepArgs)
{
    bool isDockerEnabled = IsDockerEnabledForStep(procName, task.moduleName);
    bool useDocker = false;
    QString dockerImg, additionalMounts;
    if (isDockerEnabled) {
        QString foundRoot;
        const QString &dockerImage = GetOrchestratorStepConfigValue(procName, task.moduleName, "docker_image", foundRoot);
        if (dockerImage != "") {
            // build the docker arguments
            useDocker = true;
            dockerImg = dockerImage;
            const QString &additionalDockerMounts = GetDockerAdditionalMounts(procName, task.moduleName);
            additionalMounts = GetAllDockerMounts(procName, additionalDockerMounts);
        }
    }

    QStringList newStepArgs;
    if (useDocker) {
        newStepArgs = UpdateCommandForDocker(task.moduleName, stepArgs, dockerImg, additionalMounts);
    } else {
        newStepArgs = stepArgs; // UpdateSimpleCommandArgs(task.moduleName, stepArgs)
    }

    return task.CreateStep(stepName, newStepArgs);
}

//QStringList
//StepExecutionDecorator::UpdateSimpleCommandArgs(const QString &taskName, const QStringList &arguments)
//{
//    QStringList retList;

//    const auto &modulePaths = GetModulePathMap();

//    auto it = modulePaths.find(taskName);
//    auto modulePathsEnd = std::end(modulePaths);
//    if (it == modulePathsEnd) {
//        if (arguments.size() > 0 && !arguments.at(0).startsWith('/')) {
//            // We assume in this case is an OTB application
//            retList.append("otbcli");
//        }
//    } else {
//        retList.append(it->second);
//    }
//    retList.append(arguments);

//    return retList;
//}

QStringList
StepExecutionDecorator::UpdateCommandForDocker(const QString &taskName, const QStringList &arguments, const QString& dockerImage, const QString &additionalMounts)
{
    // add the docker command parameters
    // docker run --rm -u 1003:1003 -v /mnt/archive:/mnt/archive -v /mnt/scratch:/mnt/scratch sen4cap/processors crop-type-wrapper.py
    QStringList dockerStepList = {"docker", "run", "--rm", "-u",
                                  QString::number(getuid()) + ":" + QString::number(getgid()) };
    // add also the additional mounts
    const QStringList &mounts = EnsureUniqueDockerMounts(additionalMounts);
    for (const QString &mount: mounts) {
        dockerStepList.append("-v");
        dockerStepList.append(mount);
    }
    dockerStepList.append(dockerImage);

    const auto &modulePaths = GetModulePathMap();

    auto it = modulePaths.find(taskName);
    auto modulePathsEnd = std::end(modulePaths);
    if (it == modulePathsEnd) {
        // we did not find it configured and the first item in the arguments seems like a path.
        // In this case we consider is an OTB application and add otbcli in front
        if (arguments.size() > 0 && !arguments.at(0).startsWith('/')) {
            dockerStepList.push_back("otbcli");
        }
        // otherwise, we consider the first argument is already an executable whose full path was provided
    } else {
        dockerStepList.push_back(it->second);
    }
    // add finally the command arguments
    dockerStepList.append(arguments);

    return dockerStepList;
}

bool StepExecutionDecorator::IsDockerEnabledForStep(const QString &procName, const QString &taskName) {
    QString foundRoot;
    const QString &isDockerEnabled = GetOrchestratorStepConfigValue(procName, taskName, "use_docker", foundRoot);
    if (isDockerEnabled == "") {
        // key not set, return false
        return false;
    }
    if (isDockerEnabled == "1" || QString::compare(isDockerEnabled, "true", Qt::CaseInsensitive) == 0) {
        return true;
    }
    return false;
}

QString StepExecutionDecorator::GetAllDockerMounts(const QString &procName, const QString &mounts) {
    QString allMounts = mounts;
    // add also by default the scratch path for the processor
    if (allMounts.size() > 0) {
        allMounts.append(',');
    }
    const QString &scratchPathRoot = GetScratchPathRoot(procName);
    allMounts.append(scratchPathRoot + ":" + scratchPathRoot);
    allMounts.append(",");
    allMounts.append("/mnt/archive/:/mnt/archive/");
    allMounts.append(",");
    allMounts.append(m_archiverPath + ":" + m_archiverPath);
    allMounts.append(",");
    allMounts.append(QFileInfo(m_archiverPath).canonicalFilePath() + ":" + QFileInfo(m_archiverPath).canonicalFilePath());

    return allMounts;
}

QString NormalizeMountDirName(const QString &dir) {
    QString retDir = dir.trimmed();
    return retDir.remove(QRegExp("([.|/]+)$"));
}

QStringList StepExecutionDecorator::EnsureUniqueDockerMounts(const QString &additionalMounts) {
    QStringList retList;
    if (additionalMounts.size() > 0) {
        const QStringList &mounts = additionalMounts.split(",");
        for (const QString &mount: mounts) {
            const QStringList &mountParts = mount.split(':');
            if (mountParts.size() == 2) {
                const QString &newMount = NormalizeMountDirName(mountParts[0]) +
                        ":" + NormalizeMountDirName(mountParts[1]);
                if (!retList.contains(newMount)) {
                    retList.append(newMount);
                }
            }
        }
    }
    return retList;
}

QString StepExecutionDecorator::GetDockerAdditionalMounts(const QString &procName, const QString &taskName) {
    QString mounts;
    QString key = ORCHESTRATOR_CFG_KEYS_ROOT + taskName + ".docker_add_mounts";
    QString val = GetParamValue(m_orchestratorConfig,  key, "");
    if (val != "") {
        mounts = val;
    }
    key = ORCHESTRATOR_CFG_KEYS_ROOT + procName + ".docker_add_mounts";
    val = GetParamValue(m_orchestratorConfig, key, "");
    if (val != "") {
        if (mounts.size() > 0) {
            mounts += ",";
        }
        mounts += val;
    }
    key = QString(ORCHESTRATOR_CFG_KEYS_ROOT) + "docker_add_mounts";
    val = GetParamValue(m_orchestratorConfig, key, "");
    if (val != "") {
        if (mounts.size() > 0) {
            mounts += ",";
        }
        mounts += val;
    }

    return mounts;
}

QString StepExecutionDecorator::GetOrchestratorStepConfigValue(const QString &procName, const QString &taskName, const QString &key, QString &foundRoot) {
    foundRoot = ORCHESTRATOR_CFG_KEYS_ROOT + taskName + ".";
    QString val = GetParamValue(m_orchestratorConfig,  foundRoot + key, "");
    if (val == "") {
        foundRoot = ORCHESTRATOR_CFG_KEYS_ROOT + procName + ".";
        val = GetParamValue(m_orchestratorConfig, foundRoot + key, "");
        if (val == "") {
            foundRoot = ORCHESTRATOR_CFG_KEYS_ROOT;
            val = GetParamValue(m_orchestratorConfig, foundRoot + key, "");
        }
    }
    if (val == "") {
        foundRoot = "";
    }
    return val;
}

QString StepExecutionDecorator::GetArchiverRootPath() {
    QString archPathKey(QStringLiteral("archiver.archive_path"));
    const auto &parameters = dbProvider.GetConfigurationParameters(archPathKey);
    QString archiverPath = GetParamValue(parameters, archPathKey, "/mnt/archive");
    if (archiverPath.contains('{')) {
        archiverPath = archiverPath.section("{",0,0);
        // remove also the last / character, no matter if there is or isn't something after it
        int pos = archiverPath.lastIndexOf(QChar('/'));
        archiverPath = archiverPath.left(pos);
    }

    return archiverPath;
}

QString StepExecutionDecorator::GetParamValue(const ConfigurationParameterValueList &parameters, const QString &key, const QString &defVal) {
    QString retVal = defVal;
    for (const auto &p : parameters) {
        if (p.key == key && !p.siteId) {
            retVal = p.value;
            break;
        }
    }

    return retVal;
}

QString StepExecutionDecorator::GetScratchPathRoot(const QString &procName)
{
    QString val;
    if (procName != "") {
        val  = GetParamValue(m_ScratchPathConfig, "general.scratch-path."+procName, "");
    } else {
        val = GetParamValue(m_ScratchPathConfig, QStringLiteral("general.scratch-path"), "");
    }
    Q_ASSERT(val != "");

    val = QDir::cleanPath(val) + QDir::separator();
    if (val.contains('{')) {
        val = val.split('{', QString::SkipEmptyParts).at(0);
        // remove also the last / character, no matter if there is or isn't something after it
        int pos = val.lastIndexOf(QChar('/'));
        val = val.left(pos);
    }

    return val;
}

std::map<QString, QString> StepExecutionDecorator::GetModulePathMap()
{
    std::map<QString, QString> modulePaths;
    for (const auto &p : m_modulePathsConfig) {
        modulePaths.emplace(p.key.mid(p.key.lastIndexOf('.') + 1), p.value);
    }

    return modulePaths;
}
