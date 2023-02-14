#pragma once

#include <map>
#include <vector>
#include <QFileInfo>

#include "executioncontextbase.hpp"

#include "tasktosubmit.hpp"

class EventProcessingContext : public ExecutionContextBase
{
public:
    EventProcessingContext(PersistenceManagerDBProvider &persistenceManager);

    std::map<QString, QString> GetJobConfigurationParameters(int jobId, const QString &prefix);

    int SubmitJob(const NewJob &job);

    int SubmitTask(const NewTask &task, const QString &submitterProcName);
    void SubmitSteps(const NewStepList &steps);

    void MarkJobPaused(int jobId);
    void MarkJobResumed(int jobId);
    void MarkJobCancelled(int jobId);
    void MarkJobFinished(int jobId);
    void MarkJobFailed(int jobId);
    void MarkEmptyJobFailed(int jobId, const QString &err);
    void MarkJobNeedsInput(int jobId);

    TaskIdList GetJobTasksByStatus(int jobId, const ExecutionStatusList &statusList);
    JobStepToRunList GetTaskStepsForStart(int taskId);
    JobStepToRunList GetJobStepsForResume(int jobId);

    StepConsoleOutputList GetTaskConsoleOutputs(int taskId);

    UnprocessedEventList GetNewEvents();
    void MarkEventProcessingStarted(int eventId);
    void MarkEventProcessingComplete(int eventId);

    int InsertProduct(const NewProduct &product);

    Product GetProduct(int productId);
    ProductList GetProductsForTile(int siteId, const QString &tileId, ProductType productType, int tileSatelliteId, int targetSatelliteId);
    TileList GetSiteTiles(int siteId, int satelliteId);
    TileList GetIntersectingTiles(Satellite satellite, const QString &tileId);
//    QStringList GetProductFiles(const QString &path, const QString &pattern) const;
    QString GetJobOutputPath(int jobId, const QString& procName);
    QString GetOutputPath(int jobId, int taskId, const QString &module, const QString& procName);

    void SubmitTasks(int jobId, const QList<std::reference_wrapper<TaskToSubmit>> &tasks, const QString &submitterProcName);

//    template <typename F>
//    NewStepList CreateStepsFromInput(int taskId,
//                                     const QString &inputPath,
//                                     const QString &outputPath,
//                                     const QString &pattern,
//                                     F &&f)
//    {
//        const auto &input = QDir::cleanPath(inputPath) + QDir::separator();

//        NewStepList steps;
//        for (const auto &file : GetProductFiles(inputPath, pattern)) {
//            steps.push_back(
//                { taskId, QFileInfo(file).baseName(),
//                  QString::fromUtf8(QJsonDocument(f(input + file, outputPath + file)).toJson()) });
//        }

//        return steps;
//    }

    QString GetProductAbsolutePath(int siteId, const QString &path);

private:
    QString GetScratchPath(int jobId, const QString& procName);
};
