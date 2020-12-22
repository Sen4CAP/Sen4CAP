#ifndef TAOJOBOPERATIONSSENDER_H
#define TAOJOBOPERATIONSSENDER_H

#include <QNetworkAccessManager>
#include <QNetworkReply>

#include "model.hpp"

class TaoJobOperationsSender : public QObject
{
    Q_OBJECT
public:
    TaoJobOperationsSender();
    virtual ~TaoJobOperationsSender();
    void SetServiceUrl(const QString &url);
    void SetServiceMethod(const QString &method);
    void SetServiceUser(const QString &user);
    void SetServicePass(const QString &pass);

private:
    QNetworkAccessManager networkAccessManager;
    QString serviceUrl;
    QString serviceMethod;
    QString serviceUser;
    QString servicePass;

public slots:
    void SendStartJob(QString endpointName, QString jobJson, QString callbackJson);

private slots:
    void PostFinished(QNetworkReply *reply);
    void ReadyRead();
    void PrintError(QNetworkReply *reply, const QList<QSslError> &errors);
    void OnAuthenticationRequest(QNetworkReply*,QAuthenticator*);
};

#endif // TAOJOBOPERATIONSSENDER_H
