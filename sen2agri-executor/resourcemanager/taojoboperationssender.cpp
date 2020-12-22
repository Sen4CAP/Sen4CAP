#include "taojoboperationssender.h"

#include "logger.hpp"

#include <QNetworkRequest>
#include <QNetworkReply>
#include <QAuthenticator>
#include <QUrlQuery>

TaoJobOperationsSender::TaoJobOperationsSender()
{
    connect ( &networkAccessManager,
              SIGNAL(authenticationRequired(QNetworkReply*,QAuthenticator*)),
              this,
              SLOT(OnAuthenticationRequest(QNetworkReply*,QAuthenticator*)) );

    connect(&networkAccessManager, &QNetworkAccessManager::finished, this, &TaoJobOperationsSender::PostFinished);
    connect(&networkAccessManager, &QNetworkAccessManager::sslErrors, this, &TaoJobOperationsSender::PrintError);

}

TaoJobOperationsSender::~TaoJobOperationsSender()
{

}

void TaoJobOperationsSender::SendStartJob(QString endpointName, QString jobJson, QString callbackJson)
{
    QNetworkRequest request(serviceUrl + endpointName);
    request.setHeader(QNetworkRequest::ContentTypeHeader, QStringLiteral("application/x-www-form-urlencoded"));
    request.setSslConfiguration(QSslConfiguration::defaultConfiguration());

    QUrlQuery query;
    query.addQueryItem("graph", jobJson);
    query.addQueryItem("container", "sen2agri-processors");
    query.addQueryItem("callback", callbackJson);

    QByteArray postData = query.toString(QUrl::FullyEncoded).toUtf8();

    auto reply = networkAccessManager.post(request, postData);
    connect(reply, &QNetworkReply::readyRead, this, &TaoJobOperationsSender::ReadyRead);
}

void TaoJobOperationsSender::SetServiceUrl(const QString &url)
{
    serviceUrl = url;
}

void TaoJobOperationsSender::SetServiceMethod(const QString &method)
{
    serviceMethod = method;
}

void TaoJobOperationsSender::SetServiceUser(const QString &user)
{
    serviceUser = user;
}

void TaoJobOperationsSender::SetServicePass(const QString &pass)
{
    servicePass = pass;
}


void TaoJobOperationsSender::OnAuthenticationRequest(QNetworkReply *,
                                               QAuthenticator *aAuthenticator)
{
    qDebug() << Q_FUNC_INFO;
    aAuthenticator->setUser(serviceUser);
    aAuthenticator->setPassword(servicePass);
}

void TaoJobOperationsSender::ReadyRead()
{
    auto reply = qobject_cast<QNetworkReply *>(sender());
    Q_ASSERT(reply);
    const QString &ret = reply->readAll();
    qDebug() << ret;
    std::cout << ret.toStdString() <<std::endl;
}

void TaoJobOperationsSender::PostFinished(QNetworkReply *reply)
{
    if (reply->error() != QNetworkReply::NoError) {
        Logger::error(
            QStringLiteral("Unable to submit job request: %1").arg(reply->errorString()));
    } else {
        const QString &ret = reply->readAll();
        qDebug() << ret;
        std::cout << ret.toStdString() <<std::endl;

//        const auto &obj = QJsonDocument::fromJson(reply->readAll()).object();
//        qDebug() << obj;
    }
}

void TaoJobOperationsSender::PrintError(QNetworkReply *reply, const QList<QSslError>& errors)
{
    foreach(QSslError error, errors) {
        qDebug() << error.errorString(); // error on 'errors'
        std::cout << error.errorString().toStdString() << std::endl;
    }

    reply->ignoreSslErrors();
}

