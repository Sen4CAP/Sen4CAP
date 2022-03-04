#include "stepexecutiondecorator.h"
#include "settings.hpp"
#include "configuration.hpp"
#include "logger.hpp"

#include <unistd.h>
#include <sys/types.h>

#define ORCHESTRATOR_CFG_KEYS_ROOT "general.orchestrator."
#define USE_DOCKER_STEP_CFG_KEY "use_docker"
#define DOCKER_IMAGE_STEP_CFG_KEY "docker_image"
#define DOCKER_ADD_MOUNTS_STEP_CFG_KEY "docker_add_mounts"
#define ORCHESTRATOR_TMP_PATH       "general.scratch-path"


StepExecutionDecorator::StepExecutionDecorator()
    : dbProvider(Settings::readSettings(getConfigurationFile(*QCoreApplication::instance())))
{
    m_orchestratorConfig = dbProvider.GetConfigurationParameters(ORCHESTRATOR_CFG_KEYS_ROOT);
    m_archiverPath = GetArchiverRootPath();
    m_ScratchPathConfig = dbProvider.GetConfigurationParameters(QStringLiteral(ORCHESTRATOR_TMP_PATH));
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
    QString dockerImg;
    QStringList dockerMounts;
    if (isDockerEnabled) {
        QString foundRoot;
        const QString &dockerImage = GetOrchestratorStepConfigValue(procName, task.moduleName, "docker_image", foundRoot);
        if (dockerImage != "") {
            // build the docker arguments
            useDocker = true;
            dockerImg = dockerImage;
            dockerMounts = GetDockerMounts(procName, task.moduleName);
        }
    }

    QStringList newStepArgs;
    if (useDocker) {
        newStepArgs = UpdateCommandForDocker(task.moduleName, stepArgs, dockerImg, dockerMounts);
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
StepExecutionDecorator::UpdateCommandForDocker(const QString &taskName, const QStringList &arguments, const QString& dockerImage, const QStringList &dockerMounts)
{
    // add the docker command parameters
    // docker run --rm -u 1003:1003 -v /mnt/archive:/mnt/archive -v /mnt/scratch:/mnt/scratch sen4cap/processors:2.0.0 crop-type-wrapper.py
    QStringList dockerStepList = {"docker", "run", "--rm", "-u",
                                  QString::number(getuid()) + ":" + QString::number(getgid()),
                                 "--group-add", QString::number(QFileInfo("/var/run/docker.sock").groupId())};
    // add also the additional mounts
    for (const QString &mount: dockerMounts) {
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
    const QString &isDockerEnabled = GetOrchestratorStepConfigValue(procName, taskName, USE_DOCKER_STEP_CFG_KEY, foundRoot);
    if (isDockerEnabled == "") {
        // key not set, return false
        return false;
    }
    if (isDockerEnabled == "1" || QString::compare(isDockerEnabled, "true", Qt::CaseInsensitive) == 0) {
        return true;
    }
    return false;
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
                if (!HasMount(retList, newMount)) {
                    retList.append(newMount);
                }
            } else {
                Logger::error(QStringLiteral("Mount %1 does not have 2 parts separated by column. Ignoring it ... ").arg(mount));
            }
        }
    }
    return retList;
}

bool StepExecutionDecorator::HasMount(const QStringList &mounts, const QString &mount) {
    if (mounts.contains(mount)) {
        return true;
    }
    for (const QString &mnt: mounts) {
        const QStringList &mountParts = mount.split(':');
        const QStringList &mntParts = mnt.split(':');
        if (mntParts.size() == 2 && mountParts.size() == 2) {
            // mount point already added.
            if (mntParts[1] == mountParts[1]) {
                return true;
            }
        }
    }
    return false;
}

QStringList StepExecutionDecorator::GetDockerMounts(const QString &procName, const QString &taskName) {
    // The user added additional mounts have priority in the automatically detected mounts.
    // This is why they are added first (see also the above EnsureUniqueDockerMounts and HasMount functions)
    QString mounts;
    QString key = ORCHESTRATOR_CFG_KEYS_ROOT + taskName + "." + DOCKER_ADD_MOUNTS_STEP_CFG_KEY;
    QString val = GetParamValue(m_orchestratorConfig,  key, "");
    if (val != "") {
        mounts = val;
        mounts += ",";
    }
    key = ORCHESTRATOR_CFG_KEYS_ROOT + procName + "." + DOCKER_ADD_MOUNTS_STEP_CFG_KEY;
    val = GetParamValue(m_orchestratorConfig, key, "");
    if (val != "") {
        mounts += val;
        mounts += ",";
    }
    key = QString(ORCHESTRATOR_CFG_KEYS_ROOT) + DOCKER_ADD_MOUNTS_STEP_CFG_KEY;
    val = GetParamValue(m_orchestratorConfig, key, "");
    if (val != "") {
        mounts += val;
        mounts += ",";
    }

    // Add also fixed docker mounts (scratch path, /mnt/archive, archiver path etc.)
    const QString &scratchPathRoot = GetScratchPathRoot(procName);
    mounts.append(scratchPathRoot + ":" + scratchPathRoot);
    mounts.append(",");
    mounts.append("/mnt/archive/:/mnt/archive/");
    mounts.append(",");
    mounts.append("/etc/sen2agri/:/etc/sen2agri/");
    mounts.append(",");
    mounts.append(m_archiverPath + ":" + m_archiverPath);
    mounts.append(",");
    mounts.append(QFileInfo(m_archiverPath).canonicalFilePath() + ":" + QFileInfo(m_archiverPath).canonicalFilePath());

    return EnsureUniqueDockerMounts(mounts);
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
        val  = GetParamValue(m_ScratchPathConfig, QStringLiteral(ORCHESTRATOR_TMP_PATH) + "." + procName, "");
    }
    if (val.size() == 0) {
        val = GetParamValue(m_ScratchPathConfig, QStringLiteral(ORCHESTRATOR_TMP_PATH), "");
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
