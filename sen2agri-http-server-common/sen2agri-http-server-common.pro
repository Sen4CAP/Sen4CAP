include(../common.pri)

QT -= gui
QT += core network sql dbus

TARGET = sen2agri-http-server-common
TEMPLATE = lib

CONFIG += staticlib

INCLUDEPATH += ../Optional

SOURCES += \
    qtwebapphttprequest.cpp \
    qtwebapphttpresponse.cpp \
    abstracthttpcontroller.cpp \
    httpserver.cpp \
    qtwebapphttplistener.cpp \
    qtwebapphttprequestmapper.cpp \
    requestmapperbase.cpp \
    abstracthttplistener.cpp

HEADERS += \
    pch.hpp \
    abstracthttprequest.h \
    abstracthttpresponse.h \
    qtwebapphttprequest.h \
    qtwebapphttpresponse.h \
    abstracthttpcontroller.h \
    httpserver.h \
    abstracthttplistener.h \
    qtwebapphttplistener.h \
    qtwebapphttprequestmapper.hpp \
    requestmapperbase.h

QTWEBAPP = -lQtWebApp
CONFIG(debug, debug|release) {
    QTWEBAPP = $$join(QTWEBAPP,,,d)
}

LIBS += -L$$OUT_PWD/../QtWebApp/ $$QTWEBAPP

INCLUDEPATH += $$PWD/../QtWebApp
DEPENDPATH += $$PWD/../QtWebApp

CONFIG(debug, debug|release) {
    LIBQTWEBAPP = $$OUT_PWD/../QtWebApp/libQtWebAppd.so
}
CONFIG(release, debug|release) {
    LIBQTWEBAPP = $$OUT_PWD/../QtWebApp/libQtWebApp.so
}

PRE_TARGETDEPS += $$LIBQTWEBAPP


LIBS += -L$$OUT_PWD/../sen2agri-common/ -lsen2agri-common

INCLUDEPATH += $$PWD/../sen2agri-common
DEPENDPATH += $$PWD/../sen2agri-common

PRE_TARGETDEPS += $$OUT_PWD/../sen2agri-common/libsen2agri-common.a

LIBS += -L$$OUT_PWD/../sen2agri-persistence/ -lsen2agri-persistence

INCLUDEPATH += $$PWD/../sen2agri-persistence
DEPENDPATH += $$PWD/../sen2agri-persistence

PRE_TARGETDEPS += $$OUT_PWD/../sen2agri-persistence/libsen2agri-persistence.a
