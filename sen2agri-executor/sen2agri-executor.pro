include(../common.pri)

QT       += core dbus network sql
QT       -= gui

TARGET = sen2agri-executor

DESTDIR = bin

CONFIG   += console
CONFIG   -= app_bundle

DEFINES += QT_SHARED

TEMPLATE = app

INCLUDEPATH += ../Optional

dbus_interface.files = ../dbus-interfaces/org.esa.sen2agri.processorsExecutor.xml
dbus_interface.header_flags = -i ../sen2agri-common/model.hpp

DBUS_ADAPTORS += dbus_interface

dbus_interface3.files = ../dbus-interfaces/org.esa.sen2agri.orchestrator.xml
dbus_interface3.header_flags = -i ../sen2agri-common/model.hpp

DBUS_INTERFACES += dbus_interface3

SOURCES += main.cpp \
    abstractexecinfosprotsrv.cpp \
    execinfosprotsrvfactory.cpp \
    orchestratorrequestshandler.cpp \
    persistenceitfmodule.cpp \
    simpleudpexecinfosprotsrv.cpp \
    commandinvoker.cpp \
    slurmsacctresultparser.cpp \
    processorexecutioninfos.cpp \
    processorwrapperfactory.cpp \
    configurationmgr.cpp \
    requestparamsbase.cpp \
    requestparamscanceltasks.cpp \
    requestparamssubmitsteps.cpp \
    requestparamsexecutioninfos.cpp \
    simpletcpexecinfosprotsrv.cpp \
    simpletcpexecinfoconnection.cpp \
    http/controller/executorcontroller.cpp \
    adaptor/dbusexecutoradaptor.cpp \
    adaptor/httpexecutoradaptor.cpp \
    resourcemanager/resourcemanagerfactory.cpp \
    resourcemanager/abstractresourcemanageritf.cpp \
    resourcemanager/resourcemanageritf_slurm.cpp \
    resourcemanager/resourcemanageritf_tao.cpp \
    requestparamsjobops.cpp \
    orchestratorclient/orchestratorclient.cpp \
    orchestratorclient/dbusorchestratorclient.cpp \
    orchestratorclient/httporchestratorclient.cpp \
    orchestratorclient/orchestratorclientfactory.cpp \
    resourcemanager/taojoboperationssender.cpp

HEADERS += \
    abstractexecinfosprotsrv.h \
    execinfosprotsrvfactory.h \
    orchestratorrequestshandler.h \
    persistenceitfmodule.h \
    simpleudpexecinfosprotsrv.h \
    iprocessorwrappermsgslistener.h \
    commandinvoker.h \
    slurmsacctresultparser.h \
    processorexecutioninfos.h \
    processorwrapperfactory.h \
    configurationmgr.h \
    pch.hpp \
    requestparamsbase.h \
    requestparamscanceltasks.h \
    requestparamssubmitsteps.h \
    requestparamsexecutioninfos.h \
    simpletcpexecinfosprotsrv.h \
    simpletcpexecinfoconnection.h \
    http/controller/executorcontroller.hpp \
    adaptor/dbusexecutoradaptor.h \
    adaptor/httpexecutoradaptor.h \
    resourcemanager/resourcemanagerfactory.h \
    resourcemanager/abstractresourcemanageritf.h \
    resourcemanager/resourcemanageritf_slurm.h \
    resourcemanager/resourcemanageritf_tao.h \
    requestparamsjobops.h \
    orchestratorclient/orchestratorclient.h \
    orchestratorclient/dbusorchestratorclient.h \
    orchestratorclient/httporchestratorclient.h \
    orchestratorclient/orchestratorclientfactory.h \
    resourcemanager/taojoboperationssender.h

OTHER_FILES += \
    ../dbus-interfaces/org.esa.sen2agri.processorsExecutor.xml \
    dist/org.esa.sen2agri.processorsExecutor.conf \
    dist/org.esa.sen2agri.processorsExecutor.service \
    dist/sen2agri-executor.service

target.path = /usr/bin

interface.path = /usr/share/dbus-1/interfaces
interface.files = ../dbus-interfaces/org.esa.sen2agri.executor.xml

dbus-policy.path = /etc/dbus-1/system.d
dbus-policy.files = dist/org.esa.sen2agri.processorsExecutor.conf

dbus-service.path = /usr/share/dbus-1/system-services
dbus-service.files = dist/org.esa.sen2agri.processorsExecutor.service

systemd-service.path = /usr/lib/systemd/system
systemd-service.files = dist/sen2agri-executor.service

INSTALLS += target interface dbus-policy dbus-service systemd-service

LIBS += -L$$OUT_PWD/../sen2agri-persistence/ -lsen2agri-persistence

INCLUDEPATH += $$PWD/../sen2agri-persistence
DEPENDPATH += $$PWD/../sen2agri-persistence

PRE_TARGETDEPS += $$OUT_PWD/../sen2agri-persistence/libsen2agri-persistence.a

LIBS += -L$$OUT_PWD/../sen2agri-common/ -lsen2agri-common

INCLUDEPATH += $$PWD/../sen2agri-common
DEPENDPATH += $$PWD/../sen2agri-common

PRE_TARGETDEPS += $$OUT_PWD/../sen2agri-common/libsen2agri-common.a

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


LIBS += -L$$OUT_PWD/../sen2agri-http-server-common/ -lsen2agri-http-server-common

INCLUDEPATH += $$PWD/../sen2agri-http-server-common
DEPENDPATH += $$PWD/../sen2agri-http-server-common

PRE_TARGETDEPS += $$OUT_PWD/../sen2agri-http-server-common/libsen2agri-http-server-common.a
