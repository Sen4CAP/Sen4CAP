#pragma once

#include <QNetworkAccessManager>

#include "model.hpp"
#include "executorproxy.hpp"
#include "persistencemanager.hpp"


class HttpExecutorProxy : public QObject, public ExecutorProxy
{
    Q_OBJECT

    QNetworkAccessManager networkAccessManager;
    QString serviceUrl;
    PersistenceManagerDBProvider &persistenceManager;

public:
    HttpExecutorProxy(PersistenceManagerDBProvider &persistenceMng, QObject *parent = 0);
    virtual ~HttpExecutorProxy();

    virtual void SubmitJob(int jobId);
    virtual void CancelJob(int jobId);
    virtual void PauseJob(int jobId);
    virtual void ResumeJob(int jobId);

    virtual void SubmitSteps(NewExecutorStepList steps);
    virtual void CancelTasks(TaskIdList tasks);

private:
    QByteArray GetJobIdJson(int jobId);

private:
    template <typename T>
    QString QListToJSon(const QList<T> &list)
    {
        QJsonArray array;
        for (auto & item : list)
            array.append(item.toJson());
        return QString::fromUtf8(QJsonDocument(array).toJson());
    }

private slots:
    void PostRequest(const QString &fnc, const QByteArray &payload);

private slots:
    void replyFinished();
};

