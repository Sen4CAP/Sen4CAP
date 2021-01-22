#include "json_utils.hpp"

bool getJsonValueAsInt(const QJsonObject &jsonObj, const QString &key, int &outVal) {
    bool bRet = false;
    if(jsonObj.contains(key)) {
        // first try to get it as string
        const auto &value = jsonObj[key];
        if(value.isDouble()) {
            outVal = value.toInt();
            bRet = true;
        }
        if(value.isString()) {
            outVal = value.toString().toInt(&bRet);
        }
    }
    return bRet;
}

bool getJsonValueAsString(const QJsonObject &jsonObj, const QString &key, QString &outVal) {
    bool bRet = false;
    if(jsonObj.contains(key)) {
        // first try to get it as string
        const auto &value = jsonObj[key];
        if(value.isString()) {
            outVal = value.toString();
            bRet = true;
        }
    }
    return bRet;
}
