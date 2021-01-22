#pragma once

#include <QString>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonArray>

bool getJsonValueAsInt(const QJsonObject &jsonObj, const QString &key, int &outVal);
bool getJsonValueAsString(const QJsonObject &jsonObj, const QString &key, QString &outVal);
